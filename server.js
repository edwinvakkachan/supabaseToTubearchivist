require('dotenv').config();

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

// ---------- Logging ----------
const TZ = 'Asia/Kolkata';

const ts = () =>
  new Date().toLocaleString('en-IN', {
    timeZone: TZ,
    hour12: false,
  });

const log = (...args) => console.log(`[${ts()}]`, ...args);
const warn = (...args) => console.warn(`[${ts()}] ⚠️`, ...args);
const err = (...args) => console.error(`[${ts()}] ❌`, ...args);

// ---------- Env ----------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const API_TOKEN = process.env.API_TOKEN;
const TUBE_API_URL = process.env.TUBE_API_URL;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !API_TOKEN || !TUBE_API_URL) {
  err('Missing required env vars');
  process.exit(1);
}

// ---------- Clients ----------
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

// ---------- Main Worker ----------
async function processQueue() {
  log('Checking queue...');

  const { data: items, error } = await supabase
    .from('youtube_queue')
    .select('*')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(10);

  if (error) {
    err('Failed to fetch queue:', error.message);
    return;
  }

  if (!items.length) {
    log('No pending items.');
    return;
  }

  log(`Found ${items.length} pending item(s)`);

  for (const item of items) {
    try {
      const payload = {
        data: [
          {
            youtube_id: item.youtube_id,
            status: 'pending',
          },
        ],
      };

      log(`Sending ${item.youtube_id} to TubeSync...`);

      const res = await axios.post(TUBE_API_URL, payload, {
        headers: {
          Authorization: `Token ${API_TOKEN}`,
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        timeout: 15000,
      });

      if (res.status >= 200 && res.status < 300) {
        log(`TubeSync accepted ${item.youtube_id}`);

        await supabase
          .from('youtube_queue')
          .update({
            status: 'sent',
            sent_at: new Date().toISOString(),
          })
          .eq('id', item.id);
      } else {
        throw new Error(`TubeSync returned ${res.status}`);
      }
    } catch (e) {
      err(`Failed for ${item.youtube_id}:`, e.message);

      await supabase
        .from('youtube_queue')
        .update({
          status: 'failed',
          error_message: e.message,
        })
        .eq('id', item.id);
    }
  }

  log('Queue processing complete.');
}

// ---------- Run ----------
processQueue()
  .then(() => process.exit(0))
  .catch((e) => {
    err('Worker crashed:', e);
    process.exit(1);
  });