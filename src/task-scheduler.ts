import { ChildProcess } from 'child_process';
import { CronExpressionParser } from 'cron-parser';
import fs from 'fs';
import path from 'path';

import {
  ASSISTANT_NAME,
  DATA_DIR,
  GROUPS_DIR,
  SCHEDULER_POLL_INTERVAL,
  TIMEZONE,
} from './config.js';
import {
  ContainerOutput,
  runContainerAgent,
  writeTasksSnapshot,
} from './container-runner.js';
import {
  createReviewActivity,
  createRunFileAttachments,
  getAllTasks,
  getDueTasks,
  getTaskById,
  insertRunFile,
  logTaskRun,
  logTaskRunReturningId,
  updateTask,
  updateTaskAfterRun,
} from './db.js';
import { GroupQueue } from './group-queue.js';
import { resolveGroupFolderPath } from './group-folder.js';
import { logger } from './logger.js';
import { supabase } from './supabase.js';
import { RegisteredGroup, ScheduledTask } from './types.js';

/**
 * Compute the next run time for a recurring task, anchored to the
 * task's scheduled time rather than Date.now() to prevent cumulative
 * drift on interval-based tasks.
 *
 * Co-authored-by: @community-pr-601
 */
export function computeNextRun(task: ScheduledTask): string | null {
  if (task.schedule_type === 'once') return null;

  const now = Date.now();

  if (task.schedule_type === 'cron') {
    const interval = CronExpressionParser.parse(task.schedule_value, {
      tz: TIMEZONE,
    });
    return interval.next().toISOString();
  }

  if (task.schedule_type === 'interval') {
    const ms = parseInt(task.schedule_value, 10);
    if (!ms || ms <= 0) {
      // Guard against malformed interval that would cause an infinite loop
      logger.warn(
        { taskId: task.id, value: task.schedule_value },
        'Invalid interval value',
      );
      return new Date(now + 60_000).toISOString();
    }
    // Anchor to the scheduled time, not now, to prevent drift.
    // Skip past any missed intervals so we always land in the future.
    let next = new Date(task.next_run!).getTime() + ms;
    while (next <= now) {
      next += ms;
    }
    return new Date(next).toISOString();
  }

  return null;
}

function getMimeType(ext: string): string | null {
  const mimeTypes: Record<string, string> = {
    '.md': 'text/markdown',
    '.pdf': 'application/pdf',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.svg': 'image/svg+xml',
    '.txt': 'text/plain',
    '.html': 'text/html',
    '.csv': 'text/csv',
    '.json': 'application/json',
    '.eml': 'message/rfc822',
    '.doc': 'application/msword',
    '.docx':
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx':
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.zip': 'application/zip',
  };
  return mimeTypes[ext] || null;
}

// Known files/dirs in group root that are NOT run output
const GROUP_SYSTEM_ENTRIES = new Set([
  'CLAUDE.md',
  '.DS_Store',
  'attachments',
  'content',
  'knowledge',
  'logs',
  'outreach',
  'reports',
  'tasks',
  'messages.db',
  'messages.db-wal',
  'messages.db-shm',
]);

/**
 * Collect files created in the group directory during a run
 * and move them to the output directory for upload.
 */
function collectGroupOutputFiles(
  groupFolder: string,
  runStartTime: number,
): void {
  const groupDir = path.join(GROUPS_DIR, groupFolder);
  const outputDir = path.join(DATA_DIR, 'sessions', groupFolder, 'output');
  fs.mkdirSync(outputDir, { recursive: true });

  if (!fs.existsSync(groupDir)) return;

  let entries: string[];
  try {
    entries = fs.readdirSync(groupDir);
  } catch {
    return;
  }

  for (const entry of entries) {
    if (GROUP_SYSTEM_ENTRIES.has(entry)) continue;
    if (entry.startsWith('.')) continue;

    const srcPath = path.join(groupDir, entry);
    try {
      const stat = fs.statSync(srcPath);
      if (!stat.isFile()) continue;
      // Only collect files created/modified during or after the run
      if (stat.mtimeMs < runStartTime) continue;

      const dstPath = path.join(outputDir, entry);
      fs.renameSync(srcPath, dstPath);
      logger.debug(
        { file: entry, groupFolder },
        'Collected output file from group dir',
      );
    } catch (err) {
      logger.warn({ err, file: entry }, 'Failed to collect group output file');
    }
  }
}

async function uploadRunOutputFiles(
  taskId: string,
  runId: number,
  groupFolder: string,
): Promise<void> {
  const outputDir = path.join(DATA_DIR, 'sessions', groupFolder, 'output');
  if (!fs.existsSync(outputDir)) return;

  const files = fs.readdirSync(outputDir);
  if (files.length === 0) return;

  for (const fileName of files) {
    const filePath = path.join(outputDir, fileName);
    const stat = fs.statSync(filePath);
    if (!stat.isFile()) continue;

    const fileContent = fs.readFileSync(filePath);
    const storagePath = `${taskId}/${runId}/${fileName}`;
    const ext = path.extname(fileName).toLowerCase();
    const mimeType = getMimeType(ext);

    const { error } = await supabase.storage
      .from('agent-outputs')
      .upload(storagePath, fileContent, {
        contentType: mimeType || 'application/octet-stream',
        upsert: false,
      });

    if (error) {
      logger.error({ error, storagePath }, 'Failed to upload run output file');
      continue;
    }

    await insertRunFile({
      run_id: runId,
      agent_id: taskId,
      file_name: fileName,
      mime_type: mimeType,
      file_size: stat.size,
      storage_path: storagePath,
    });
  }

  // Clean up output directory after successful upload
  for (const fileName of files) {
    const filePath = path.join(outputDir, fileName);
    try {
      fs.unlinkSync(filePath);
    } catch {}
  }
}

export interface SchedulerDependencies {
  registeredGroups: () => Record<string, RegisteredGroup>;
  getSessions: () => Record<string, string>;
  queue: GroupQueue;
  onProcess: (
    groupJid: string,
    proc: ChildProcess,
    containerName: string,
    groupFolder: string,
  ) => void;
  sendMessage: (jid: string, text: string) => Promise<void>;
}

async function runTask(
  task: ScheduledTask,
  deps: SchedulerDependencies,
): Promise<void> {
  const startTime = Date.now();
  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(task.group_folder);
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    // Stop retry churn for malformed legacy rows.
    await updateTask(task.id, { status: 'paused' });
    logger.error(
      { taskId: task.id, groupFolder: task.group_folder, error },
      'Task has invalid group folder',
    );
    await logTaskRun({
      task_id: task.id,
      run_at: new Date().toISOString(),
      duration_ms: Date.now() - startTime,
      status: 'error',
      result: null,
      error,
    });
    return;
  }
  fs.mkdirSync(groupDir, { recursive: true });

  logger.info(
    { taskId: task.id, group: task.group_folder },
    'Running scheduled task',
  );

  const groups = deps.registeredGroups();
  const group = Object.values(groups).find(
    (g) => g.folder === task.group_folder,
  );

  if (!group) {
    logger.error(
      { taskId: task.id, groupFolder: task.group_folder },
      'Group not found for task',
    );
    await logTaskRun({
      task_id: task.id,
      run_at: new Date().toISOString(),
      duration_ms: Date.now() - startTime,
      status: 'error',
      result: null,
      error: `Group not found: ${task.group_folder}`,
    });
    return;
  }

  // Update tasks snapshot for container to read (filtered by group)
  const isMain = group.isMain === true;
  const tasks = await getAllTasks();
  writeTasksSnapshot(
    task.group_folder,
    isMain,
    tasks.map((t) => ({
      id: t.id,
      groupFolder: t.group_folder,
      prompt: t.prompt,
      schedule_type: t.schedule_type,
      schedule_value: t.schedule_value,
      status: t.status,
      next_run: t.next_run,
    })),
  );

  let result: string | null = null;
  let error: string | null = null;

  // For group context mode, use the group's current session
  const sessions = deps.getSessions();
  const sessionId =
    task.context_mode === 'group' ? sessions[task.group_folder] : undefined;

  // After the task produces a result, close the container promptly.
  // Tasks are single-turn — no need to wait IDLE_TIMEOUT (30 min) for the
  // query loop to time out. A short delay handles any final MCP calls.
  const TASK_CLOSE_DELAY_MS = 10000;
  let closeTimer: ReturnType<typeof setTimeout> | null = null;

  const scheduleClose = () => {
    if (closeTimer) return; // already scheduled
    closeTimer = setTimeout(() => {
      logger.debug({ taskId: task.id }, 'Closing task container after result');
      deps.queue.closeStdin(task.chat_jid);
    }, TASK_CLOSE_DELAY_MS);
  };

  try {
    const output = await runContainerAgent(
      group,
      {
        prompt: task.prompt,
        sessionId,
        groupFolder: task.group_folder,
        chatJid: task.chat_jid,
        isMain,
        isScheduledTask: true,
        assistantName: ASSISTANT_NAME,
      },
      (proc, containerName) =>
        deps.onProcess(task.chat_jid, proc, containerName, task.group_folder),
      async (streamedOutput: ContainerOutput) => {
        if (streamedOutput.result) {
          result = streamedOutput.result;
          // Forward result to user (sendMessage handles formatting)
          await deps.sendMessage(task.chat_jid, streamedOutput.result);
          scheduleClose();
        }
        if (streamedOutput.status === 'success') {
          deps.queue.notifyIdle(task.chat_jid);
          scheduleClose(); // Close promptly even when result is null (e.g. IPC-only tasks)
        }
        if (streamedOutput.status === 'error') {
          error = streamedOutput.error || 'Unknown error';
        }
      },
    );

    if (closeTimer) clearTimeout(closeTimer);

    if (output.status === 'error') {
      error = output.error || 'Unknown error';
    } else if (output.result) {
      // Result was already forwarded to the user via the streaming callback above
      result = output.result;
    }

    logger.info(
      { taskId: task.id, durationMs: Date.now() - startTime },
      'Task completed',
    );
  } catch (err) {
    if (closeTimer) clearTimeout(closeTimer);
    error = err instanceof Error ? err.message : String(err);
    logger.error({ taskId: task.id, error }, 'Task failed');
  }

  const durationMs = Date.now() - startTime;

  // Read any sent message copies from the output directory.
  // The container's send_message tool saves text copies as sent-*.md files
  // so we can capture the full output (not just the final summary).
  const outputDir = path.join(DATA_DIR, 'sessions', task.group_folder, 'output');
  if (fs.existsSync(outputDir)) {
    const sentFiles = fs
      .readdirSync(outputDir)
      .filter((f) => f.startsWith('sent-') && f.endsWith('.md'))
      .sort();
    if (sentFiles.length > 0) {
      const sentMessages: string[] = [];
      for (const f of sentFiles) {
        try {
          const content = fs.readFileSync(path.join(outputDir, f), 'utf-8');
          if (content.trim()) sentMessages.push(content);
        } catch {}
      }
      if (sentMessages.length > 0) {
        const fullOutput = sentMessages.join('\n\n---\n\n');
        result = result ? `${fullOutput}\n\n---\n\n${result}` : fullOutput;
      }
    }
  }

  const taskStatus = error ? 'error' : 'success';

  const runId = await logTaskRunReturningId({
    task_id: task.id,
    run_at: new Date().toISOString(),
    duration_ms: durationMs,
    status: taskStatus,
    result,
    error,
  });

  // Save full agent response as markdown file for upload
  if (result && runId) {
    fs.mkdirSync(outputDir, { recursive: true });
    const responseFile = path.join(outputDir, `response-${Date.now()}.md`);
    fs.writeFileSync(responseFile, result);
  }

  if (runId && !error) {
    // Collect files created in the group dir during this run (e.g. generated images)
    collectGroupOutputFiles(task.group_folder, startTime);
    await uploadRunOutputFiles(task.id, runId, task.group_folder);
  }

  // Create CRM review task and link file attachments
  if (runId) {
    const activityId = await createReviewActivity(
      task.name || task.prompt.slice(0, 80),
      runId,
      task.id,
      taskStatus,
      result,
    );
    if (activityId) {
      await createRunFileAttachments(activityId, runId);
    }
  }

  const nextRun = computeNextRun(task);
  const resultSummary = error
    ? `Error: ${error}`
    : result
      ? result.slice(0, 200)
      : 'Completed';
  await updateTaskAfterRun(task.id, nextRun, resultSummary);
}

let schedulerRunning = false;

export function startSchedulerLoop(deps: SchedulerDependencies): void {
  if (schedulerRunning) {
    logger.debug('Scheduler loop already running, skipping duplicate start');
    return;
  }
  schedulerRunning = true;
  logger.info('Scheduler loop started');

  const loop = async () => {
    try {
      const dueTasks = await getDueTasks();
      if (dueTasks.length > 0) {
        logger.info({ count: dueTasks.length }, 'Found due tasks');
      }

      for (const task of dueTasks) {
        // Re-check task status in case it was paused/cancelled
        const currentTask = await getTaskById(task.id);
        if (!currentTask || currentTask.status !== 'active') {
          continue;
        }

        deps.queue.enqueueTask(currentTask.chat_jid, currentTask.id, () =>
          runTask(currentTask, deps),
        );
      }
    } catch (err) {
      logger.error({ err }, 'Error in scheduler loop');
    }

    setTimeout(loop, SCHEDULER_POLL_INTERVAL);
  };

  loop();
}

/** @internal - for tests only. */
export function _resetSchedulerLoopForTests(): void {
  schedulerRunning = false;
}
