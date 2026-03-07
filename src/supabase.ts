import { createClient } from '@supabase/supabase-js';

import { readEnvFile } from './env.js';

const env = readEnvFile(['SUPABASE_URL', 'SUPABASE_SERVICE_KEY']);

const url = process.env.SUPABASE_URL || env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY || env.SUPABASE_SERVICE_KEY;

if (!url || !key) {
  throw new Error(
    'SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env or environment',
  );
}

export const supabase = createClient(url, key, {
  db: { schema: 'nanoclaw' },
});
