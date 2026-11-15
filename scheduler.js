import cron from "node-cron";
import { spawn } from "node:child_process";
import { appendFile, mkdir, readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const PROJECT_ROOT = path.dirname(fileURLToPath(import.meta.url));
const RUN_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

class StageError extends Error {
  constructor(stage, message, details = {}) {
    super(message);
    this.name = "StageError";
    this.stage = stage;
    Object.assign(this, details);
  }
}

function safeError(value) {
  return String(value ?? "")
    .replace(/(postgres(?:ql)?:\/\/[^:\s]+:)[^@\s]+@/gi, "$1[REDACTED]@")
    .replace(/(password|api[_-]?key|token|secret)\s*[:=]\s*[^\s,;]+/gi, "$1=[REDACTED]")
    .slice(-2000);
}

function runtimeConfig() {
  const dataDir = path.resolve(PROJECT_ROOT, process.env.PIPELINE_DATA_DIR || path.join("pipeline", "data"));
  const python = (process.env.PIPELINE_PYTHON || "python").trim();
  if (!python) throw new Error("PIPELINE_PYTHON must name the Python executable.");
  const schedulerLog = path.resolve(
    PROJECT_ROOT,
    process.env.PREPAPIG_SCHEDULER_LOG || path.join(dataDir, "logs", "scheduler", "runs.jsonl"),
  );
  return { dataDir, python, schedulerLog };
}

function config() {
  const runtime = runtimeConfig();
  const dailyTime = (process.env.PREPAPIG_DAILY_TIME ?? "").trim();
  const timezone = (process.env.PREPAPIG_TIMEZONE ?? "").trim();
  if (!/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(dailyTime)) {
    throw new Error("Set PREPAPIG_DAILY_TIME as HH:MM before starting the scheduler.");
  }
  if (!timezone) throw new Error("Set PREPAPIG_TIMEZONE to an IANA timezone before starting the scheduler.");
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone }).format(new Date());
  } catch {
    throw new Error("PREPAPIG_TIMEZONE is not a recognized IANA timezone.");
  }
  const [hour, minute] = dailyTime.split(":").map(Number);
  const expression = `${minute} ${hour} * * *`;
  if (!cron.validate(expression)) throw new Error("The configured daily schedule is not a valid cron expression.");
  return { ...runtime, dailyTime, timezone, expression };
}

function runCommand(python, args, stage) {
  return new Promise((resolve, reject) => {
    const child = spawn(python, args, { cwd: PROJECT_ROOT, env: process.env, shell: false, windowsHide: true });
    let stdout = "";
    let stderr = "";
    const keepTail = (current, chunk) => `${current}${chunk.toString("utf8")}`.slice(-12000);
    child.stdout.on("data", (chunk) => { stdout = keepTail(stdout, chunk); });
    child.stderr.on("data", (chunk) => { stderr = keepTail(stderr, chunk); });
    child.on("error", (error) => reject(new StageError(stage, safeError(error.message), { cause: error })));
    child.on("close", (code, signal) => {
      if (code === 0) return resolve({ stdout, stderr });
      reject(new StageError(stage, `Python stage exited with code ${code ?? "unknown"}${signal ? ` (${signal})` : ""}: ${safeError(stderr || stdout)}`, { exitCode: code }));
    });
  });
}

async function readJson(file) {
  return JSON.parse(await readFile(file, "utf8"));
}

async function findExtractionAudit(dataDir, startedAtMs) {
  const directory = path.join(dataDir, "extraction", "audit");
  let names;
  try { names = await readdir(directory); }
  catch { return null; }
  const candidates = [];
  for (const name of names.filter((item) => item.endsWith(".json"))) {
    const file = path.join(directory, name);
    try {
      const details = await stat(file);
      if (details.mtimeMs < startedAtMs - 10000) continue;
      const metadata = await readJson(file);
      if (!metadata.pipeline_run_id || !metadata.extraction_start) continue;
      const runStart = Date.parse(metadata.extraction_start);
      if (Number.isFinite(runStart) && runStart >= startedAtMs - 10000) candidates.push({ file, metadata, runStart });
    } catch {
      // Ignore an incomplete/stale audit file; the extraction process records its own final file.
    }
  }
  candidates.sort((a, b) => b.runStart - a.runStart);
  return candidates[0] ?? null;
}

async function appendAudit(settings, record) {
  await mkdir(path.dirname(settings.schedulerLog), { recursive: true });
  await appendFile(settings.schedulerLog, `${JSON.stringify(record)}\n`, "utf8");
}

async function runDownstream(settings, extraction, record) {
  const pipelineRunId = String(extraction.pipeline_run_id ?? "");
  if (!RUN_ID_PATTERN.test(pipelineRunId) || pipelineRunId.includes("..")) {
    throw new StageError("handover", "Extraction metadata contains an unsafe pipeline_run_id.");
  }
  record.pipeline_run_id = pipelineRunId;
  record.stage_results.extraction = extraction.extraction_status === "SUCCESS"
    ? (record.mode === "retry" ? "REUSED_SUCCESS" : "SUCCESS")
    : "FAILED";
  record.stage_results.handover = "RUNNING";
  if (extraction.extraction_status !== "SUCCESS" || extraction.handover_decision !== "APPROVED" || extraction.validation_result !== "PASS") {
    record.stage_results.handover = "FAILED";
    throw new StageError("handover", `Extraction ${pipelineRunId} is not SUCCESS/PASS/APPROVED; downstream stages were not started.`, { pipelineRunId });
  }
  record.stage_results.handover = "SUCCESS";
  const inputDir = path.join(settings.dataDir, "raw", pipelineRunId);
  const metadataPath = path.join(settings.dataDir, "extraction", "audit", `${pipelineRunId}.json`);
  record.stage_results.transformation = "RUNNING";
  await runCommand(settings.python, ["-m", "pipeline.transformation.orchestrate", "--input-dir", inputDir, "--metadata", metadataPath, "--output-dir", settings.dataDir], "transformation");
  const bundle = path.join(settings.dataDir, "transformed", pipelineRunId);
  let transformed;
  try { transformed = await readJson(path.join(bundle, "transformation_metadata.json")); }
  catch (error) { throw new StageError("transformation", `Transformation output metadata is unavailable: ${safeError(error.message)}`, { pipelineRunId }); }
  if (transformed.pipeline_run_id !== pipelineRunId || transformed.handover_decision !== "APPROVED" || transformed.transformation_status !== "PASS" || Number(transformed.quarantined_rows) !== 0) {
    throw new StageError("transformation", `Transformation output for ${pipelineRunId} is not load-approved.`, { pipelineRunId, transformationRunId: transformed.transformation_run_id });
  }
  record.transformation_run_id = transformed.transformation_run_id;
  record.stage_results.transformation = "SUCCESS";
  record.stage_results.load = "RUNNING";
  const { stdout } = await runCommand(settings.python, ["-m", "pipeline.load", bundle], "load");
  let loaded;
  try { loaded = JSON.parse(stdout.trim()); }
  catch { throw new StageError("load", "Load command returned success without a valid JSON load receipt.", { pipelineRunId, transformationRunId: record.transformation_run_id }); }
  if (loaded.load_status !== "SUCCESS" || loaded.pipeline_run_id !== pipelineRunId || loaded.transformation_run_id !== record.transformation_run_id) {
    throw new StageError("load", "Load receipt does not match the approved extraction/transformation run.", { pipelineRunId, transformationRunId: record.transformation_run_id });
  }
  record.load_run_id = loaded.load_run_id;
  record.stage_results.load = "SUCCESS";
}

async function executeRun(mode, retryRunId = null) {
  const settings = runtimeConfig();
  const startedAt = new Date();
  const record = {
    scheduler_run_id: randomUUID(), mode, status: "FAILED", failure_stage: "",
    started_at: startedAt.toISOString(), ended_at: "", pipeline_run_id: "",
    transformation_run_id: "", load_run_id: "", retry_of_pipeline_run_id: retryRunId || "",
    stage_results: { extraction: "NOT_STARTED", handover: "NOT_STARTED", transformation: "NOT_STARTED", load: "NOT_STARTED" },
    error_message: "",
  };
  try {
    let extraction;
    if (mode === "retry") {
      if (!RUN_ID_PATTERN.test(retryRunId || "") || retryRunId.includes("..")) throw new StageError("retry_input", "Retry requires a safe extraction pipeline_run_id.");
      const metadataPath = path.join(settings.dataDir, "extraction", "audit", `${retryRunId}.json`);
      try { extraction = await readJson(metadataPath); }
      catch (error) { throw new StageError("retry_input", `Cannot read the original extraction audit: ${safeError(error.message)}`, { pipelineRunId: retryRunId }); }
      if (extraction.pipeline_run_id !== retryRunId || extraction.extraction_status !== "SUCCESS" || extraction.handover_decision !== "APPROVED" || extraction.validation_result !== "PASS") {
        throw new StageError("retry_input", "Retry requires an unchanged SUCCESS/PASS/APPROVED extraction run.", { pipelineRunId: retryRunId });
      }
    } else {
      const extractionStart = Date.now();
      record.stage_results.extraction = "RUNNING";
      let extractionError = null;
      try {
        await runCommand(settings.python, ["-m", "pipeline.extraction.extraction", "--output-dir", settings.dataDir.toString(), "--quiet"], "extraction");
      } catch (error) { extractionError = error; }
      const found = await findExtractionAudit(settings.dataDir, extractionStart);
      if (found) {
        extraction = found.metadata;
        record.pipeline_run_id = String(extraction.pipeline_run_id || "");
      }
      if (extractionError) throw extractionError;
      if (!extraction) throw new StageError("extraction", "Extraction exited successfully but produced no new audit record.");
    }
    await runDownstream(settings, extraction, record);
    record.status = "SUCCESS";
  } catch (error) {
    record.failure_stage = error.stage || "scheduler";
    record.pipeline_run_id = error.pipelineRunId || record.pipeline_run_id;
    record.transformation_run_id = error.transformationRunId || record.transformation_run_id;
    record.error_message = safeError(error.message || error);
    if (record.failure_stage in record.stage_results && record.stage_results[record.failure_stage] === "RUNNING") {
      record.stage_results[record.failure_stage] = "FAILED";
    }
  }
  record.ended_at = new Date().toISOString();
  try { await appendAudit(settings, record); }
  catch (error) { console.error(`Could not append scheduler audit: ${safeError(error.message)}`); }
  if (record.status === "SUCCESS") console.log(JSON.stringify(record));
  else console.error(JSON.stringify(record));
  return record;
}

async function main() {
  const retryAt = process.argv.indexOf("--retry-run");
  if (retryAt >= 0) {
    const runId = process.argv[retryAt + 1];
    if (!runId) throw new Error("Usage: npm run retry -- <pipeline_run_id>");
    const result = await executeRun("retry", runId);
    if (result.status !== "SUCCESS") process.exitCode = 1;
    return;
  }
  const settings = config();
  if (process.argv.includes("--validate-config")) {
    console.log(JSON.stringify({ status: "VALID", cron: settings.expression, timezone: settings.timezone, data_dir: settings.dataDir }));
    return;
  }
  cron.schedule(settings.expression, () => executeRun("scheduled"), {
    timezone: settings.timezone,
    noOverlap: true,
  });
  console.log(`PrepAPig daily pipeline scheduled at ${settings.dailyTime} (${settings.timezone}); retry with npm run retry -- <pipeline_run_id>.`);
}

const isDirectExecution = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectExecution) {
  main().catch((error) => {
    console.error(`Scheduler configuration/run failed: ${safeError(error.message || error)}`);
    process.exitCode = 1;
  });
}

export { config, executeRun, runDownstream, safeError };
