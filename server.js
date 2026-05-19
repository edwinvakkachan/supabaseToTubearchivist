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

// ---------- Helpers ----------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
  log('Starting queue worker...');

  while (true) {
    try {
      const { data: items, error } = await supabase
        .from('youtube_queue')
        .select('*')
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(50);

      if (error) {
        err('Failed to fetch queue:', error.message);
        await sleep(10000);
        continue;
      }

      if (!items || items.length === 0) {
        log('Queue empty...');
        await sleep(5000);
        continue;
      }

      log(`Found ${items.length} pending item(s)`);

      for (const item of items) {
        try {
          // Mark processing first
          await supabase
            .from('youtube_queue')
            .update({
              status: 'processing',
            })
            .eq('id', item.id);

          const payload = {
            data: [
              {
                youtube_id: item.youtube_id,
                status: 'pending',
              },
            ],
          };

          log(`Sending ${item.youtube_id} to TubeArchivist...`);

          const res = await axios.post(
            TUBE_API_URL,
            payload,
            {
              headers: {
                Authorization: `Token ${API_TOKEN}`,
                'Content-Type': 'application/json',
                Accept: 'application/json',
              },
              timeout: 30000,
            }
          );

          if (res.status >= 200 && res.status < 300) {
            log(`TubeArchivist accepted ${item.youtube_id}`);

            await supabase
              .from('youtube_queue')
              .update({
                status: 'sent',
                sent_at: new Date().toISOString(),
                error_message: null,
              })
              .eq('id', item.id);
          } else {
            throw new Error(`TubeArchivist returned ${res.status}`);
          }

        } catch (e) {
          const errorMessage =
            e.response?.data
              ? JSON.stringify(e.response.data)
              : e.message;

          // Already exists handling
          if (errorMessage.toLowerCase().includes('already')) {
            warn(`${item.youtube_id} already exists in TubeArchivist`);

            await supabase
              .from('youtube_queue')
              .update({
                status: 'sent',
                sent_at: new Date().toISOString(),
                error_message: null,
              })
              .eq('id', item.id);

            continue;
          }

          err(`Failed for ${item.youtube_id}:`, errorMessage);

          await supabase
            .from('youtube_queue')
            .update({
              status: 'failed',
              error_message: errorMessage,
            })
            .eq('id', item.id);
        }

        // Small delay between requests
        await sleep(3000);
      }

    } catch (e) {
      err('Worker loop crashed:', e.message);
      await sleep(10000);
    }
  }
}

// ---------- Run ----------
processQueue().catch((e) => {
  err('Fatal worker crash:', e);
});