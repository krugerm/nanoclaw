/**
 * One-time migration: SQLite scheduled_tasks + task_run_logs → Supabase nanoclaw schema.
 *
 * Usage: npx tsx scripts/migrate-tasks-to-supabase.ts
 */

import Database from 'better-sqlite3';
import { createClient } from '@supabase/supabase-js';
import path from 'path';
import fs from 'fs';

// Read .env manually (same logic as src/env.ts)
function readEnv(keys: string[]): Record<string, string> {
  const envFile = path.join(process.cwd(), '.env');
  if (!fs.existsSync(envFile)) return {};
  const content = fs.readFileSync(envFile, 'utf-8');
  const result: Record<string, string> = {};
  const wanted = new Set(keys);
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    if (!wanted.has(key)) continue;
    let value = trimmed.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (value) result[key] = value;
  }
  return result;
}

const env = readEnv(['SUPABASE_URL', 'SUPABASE_SERVICE_KEY']);
const url = process.env.SUPABASE_URL || env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_KEY;

if (!url || !key) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env');
  process.exit(1);
}

const supabase = createClient(url, key, { db: { schema: 'nanoclaw' } });

// Open SQLite
const dbPath = path.join(process.cwd(), 'store', 'messages.db');
if (!fs.existsSync(dbPath)) {
  console.error(`SQLite database not found: ${dbPath}`);
  process.exit(1);
}
const db = new Database(dbPath, { readonly: true });

interface SqliteTask {
  id: string;
  group_folder: string;
  chat_jid: string;
  prompt: string;
  schedule_type: string;
  schedule_value: string;
  context_mode?: string;
  next_run: string | null;
  last_run: string | null;
  last_result: string | null;
  status: string;
  created_at: string;
}

interface SqliteRunLog {
  id: number;
  task_id: string;
  run_at: string;
  duration_ms: number;
  status: string;
  result: string | null;
  error: string | null;
}

async function migrate() {
  // 1. Migrate scheduled_tasks → nanoclaw.agents
  const tasks = db
    .prepare('SELECT * FROM scheduled_tasks')
    .all() as SqliteTask[];

  console.log(`Found ${tasks.length} tasks in SQLite`);

  if (tasks.length > 0) {
    const agents = tasks.map((t) => ({
      id: t.id,
      group_folder: t.group_folder,
      chat_jid: t.chat_jid,
      prompt: t.prompt,
      schedule_type: t.schedule_type,
      schedule_value: t.schedule_value,
      context_mode: t.context_mode || 'isolated',
      next_run: t.next_run,
      last_run: t.last_run,
      last_result: t.last_result,
      status: t.status,
      created_at: t.created_at,
    }));

    const { error } = await supabase.from('agents').upsert(agents, {
      onConflict: 'id',
    });

    if (error) {
      console.error('Failed to migrate agents:', error.message);
      process.exit(1);
    }
    console.log(`Migrated ${agents.length} agents to Supabase`);
  }

  // 2. Migrate task_run_logs → nanoclaw.agent_runs
  const logs = db
    .prepare('SELECT * FROM task_run_logs ORDER BY run_at')
    .all() as SqliteRunLog[];

  console.log(`Found ${logs.length} run logs in SQLite`);

  if (logs.length > 0) {
    // Insert in batches of 100
    const batchSize = 100;
    let migrated = 0;
    for (let i = 0; i < logs.length; i += batchSize) {
      const batch = logs.slice(i, i + batchSize).map((l) => ({
        agent_id: l.task_id,
        run_at: l.run_at,
        duration_ms: l.duration_ms,
        status: l.status,
        result: l.result,
        error: l.error,
      }));

      const { error } = await supabase.from('agent_runs').insert(batch);
      if (error) {
        console.error(
          `Failed to migrate run logs batch ${i}:`,
          error.message,
        );
        process.exit(1);
      }
      migrated += batch.length;
    }
    console.log(`Migrated ${migrated} run logs to Supabase`);
  }

  db.close();
  console.log('Migration complete.');
}

migrate().catch((err) => {
  console.error('Migration failed:', err);
  process.exit(1);
});
