
import "dotenv/config";
import axios from "axios";

import { initDB } from "./supabase/db.js";
import pool from "./supabase/pool.js";
import { delay } from "./delay.js";
import { deleteOldSentYouTubeQueue } from "./supabase/deleteOldSentYouTubeQueue.js";

const API_TOKEN = process.env.API_TOKEN;
const TUBE_API_URL = process.env.TUBE_API_URL;

const BATCH_SIZE = 50;
const REQUEST_DELAY_MS = 3000;
const LOOP_DELAY_MS = 60000;
const ERROR_DELAY_MS = 10000;

const MAX_ATTEMPTS = 4;
const RETRY_DELAY_SECONDS = 60;

// ---------- Logging ----------

function logError(context, error) {
  console.error(`❌ ${context}`);
  console.error("Message:", error?.message || "(no message)");
  console.error("Code:", error?.code || "(none)");
  console.error("Cause:", error?.cause || "(none)");
  console.error("Full error:", error);
  console.error("Stack:", error?.stack || "(no stack)");
}

async function runTask(name, task) {
  try {
    await task();
    console.log(`✅ ${name} completed`);
    return true;
  } catch (error) {
    logError(name, error);
    return false;
  }
}

// ---------- Queue status updates ----------

async function updateQueueStatus(id, status) {
  if (status === "sent") {
    await pool.query(
      `UPDATE public.youtube_queue
       SET status = 'sent',
           sent_at = NOW(),
           error_message = NULL
       WHERE id = $1`,
      [id]
    );
  }
}

// ---------- Process one queue item ----------

async function processItem(item) {
  console.log(
    `\n🎬 Processing ${item.youtube_id} (ID: ${item.id})`
  );

  // Mark as processing before sending.
  try {
    await pool.query(
      `UPDATE public.youtube_queue
       SET status = 'processing'
       WHERE id = $1`,
      [item.id]
    );
  } catch (error) {
    logError(
      `Could not mark ${item.youtube_id} as processing`,
      error
    );
    return false;
  }

  try {
    const response = await axios.post(
      TUBE_API_URL,
      {
        data: [
          {
            youtube_id: item.youtube_id,
            status: "pending",
          },
        ],
      },
      {
        headers: {
          Authorization: `Token ${API_TOKEN}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        timeout: 30000,
      }
    );

    if (response.status < 200 || response.status >= 300) {
      throw new Error(
        `TubeArchivist returned HTTP ${response.status}`
      );
    }

    await updateQueueStatus(item.id, "sent");

    console.log(
      `✅ ${item.youtube_id} accepted and marked as sent`
    );

    return true;
  } catch (error) {
    const errorMessage = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message || String(error);

    // Treat an already-existing video as successfully handled.
    if (errorMessage.toLowerCase().includes("already")) {
      console.warn(
        `⚠️ ${item.youtube_id} already exists in TubeArchivist`
      );

      try {
        await updateQueueStatus(item.id, "sent");
        return true;
      } catch (dbError) {
        logError(
          `Could not mark ${item.youtube_id} as sent`,
          dbError
        );
        return false;
      }
    }

    logError(`Failed to process ${item.youtube_id}`, error);

    // Increment retry count and mark for deletion after max attempts.
    try {
      const result = await pool.query(
        `UPDATE public.youtube_queue
         SET retry_count = COALESCE(retry_count, 0) + 1,
             last_retry = NOW(),
             status = CASE
               WHEN COALESCE(retry_count, 0) + 1 >= $1
                 THEN 'delete'
               ELSE 'failed'
             END,
             error_message = $2
         WHERE id = $3
         RETURNING retry_count, status`,
        [MAX_ATTEMPTS, errorMessage, item.id]
      );

      const updated = result.rows[0];

      if (updated?.status === "delete") {
        console.error(
          `🗑️ ${item.youtube_id} reached ${MAX_ATTEMPTS} ` +
          `failed attempts. Marked for deletion.`
        );
      } else if (updated) {
        console.warn(
          `🔁 ${item.youtube_id}: attempt ` +
          `${updated.retry_count}/${MAX_ATTEMPTS}. ` +
          `Retry after ${RETRY_DELAY_SECONDS} seconds.`
        );
      }
    } catch (dbError) {
      logError(
        `Could not save retry information for ${item.youtube_id}`,
        dbError
      );
    }

    return false;
  }
}

// ---------- Main worker ----------

async function processQueue() {
  console.log("\n======================================");
  console.log("🔄 SYNC WORKER STARTED");
  console.log("======================================");

  if (!API_TOKEN || !TUBE_API_URL) {
    throw new Error(
      "API_TOKEN or TUBE_API_URL is missing from .env"
    );
  }

  while (true) {
    try {
      const initialized = await runTask(
        "Initialize database",
        () => initDB()
      );

      if (!initialized) {
        await delay(ERROR_DELAY_MS);
        continue;
      }

      let items;

      try {
        const result = await pool.query(
          `SELECT id, youtube_id, status, created_at,
                  retry_count, last_retry
           FROM public.youtube_queue
           WHERE
             status = 'pending'
             OR (
               status = 'failed'
               AND COALESCE(retry_count, 0) < $1
               AND (
                 last_retry IS NULL
                 OR last_retry <= NOW()
                    - ($2 * INTERVAL '1 second')
               )
             )
           ORDER BY created_at ASC
           LIMIT $3`,
          [
            MAX_ATTEMPTS,
            RETRY_DELAY_SECONDS,
            BATCH_SIZE,
          ]
        );

        items = result.rows;
      } catch (error) {
        logError("Could not fetch queue items", error);
        await delay(ERROR_DELAY_MS);
        continue;
      }

      if (items.length === 0) {
        console.log("📭 No pending or retryable items");
      } else {
        console.log(`📋 Found ${items.length} queue item(s)`);

        for (const item of items) {
          try {
            await processItem(item);
          } catch (error) {
            logError(
              `Unexpected error for queue ID ${item.id}`,
              error
            );
          }

          try {
            await delay(REQUEST_DELAY_MS);
          } catch (error) {
            logError("Request delay failed", error);
          }
        }
      }

      // Cleanup runs even when individual queue items fail.
      await runTask(
        "Delete old sent YouTube queue rows",
        () => deleteOldSentYouTubeQueue()
      );

      await delay(LOOP_DELAY_MS);
    } catch (error) {
      logError("Unexpected worker-loop error", error);

      try {
        await delay(ERROR_DELAY_MS);
      } catch (delayError) {
        logError("Recovery delay failed", delayError);
      }
    }
  }
}

// ---------- Start ----------

processQueue().catch((error) => {
  logError("Fatal worker error", error);
  process.exitCode = 1;
});
