
import "dotenv/config";
import axios from "axios";
import { MongoClient } from "mongodb";

import { initDB } from "./supabase/db.js";
import pool from "./supabase/pool.js";
import { delay } from "./delay.js";
import { deleteOldSentYouTubeQueue } from "./supabase/deleteOldSentYouTubeQueue.js";

const API_TOKEN = process.env.API_TOKEN;
const TUBE_API_URL = process.env.TUBE_API_URL;
const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DATABASE = process.env.MONGODB_DATABASE || "test";
const MONGODB_COLLECTION = process.env.MONGODB_COLLECTION || "youtubeque";

const BATCH_SIZE = 50;
const REQUEST_DELAY_MS = 3000;
const LOOP_DELAY_MS = 60000;
const ERROR_DELAY_MS = 10000;
const SUBSCRIPTION_VERIFY_TIMEOUT_MS = 60000;
const SUBSCRIPTION_VERIFY_INTERVAL_MS = 3000;

const MAX_ATTEMPTS = 4;
const RETRY_DELAY_SECONDS = 60;

let mongoClient;

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

async function getMongoQueue() {
  if (!MONGODB_URI) return null;
  if (!mongoClient) {
    mongoClient = new MongoClient(MONGODB_URI);
    await mongoClient.connect();
    console.log(`✅ MongoDB connected (${MONGODB_DATABASE}.${MONGODB_COLLECTION})`);
  }
  return mongoClient.db(MONGODB_DATABASE).collection(MONGODB_COLLECTION);
}

async function processMongoItem(collection, item) {
  const youtubeId = item.youtubeId;
  console.log(`\n🎬 Processing MongoDB ${youtubeId} (ID: ${item._id})`);

  try {
    await collection.updateOne(
      { _id: item._id, status: { $in: ["PENDING", "FAILED"] } },
      { $set: { status: "PROCESSING", updatedAt: new Date() } }
    );

    const response = await axios.post(
      TUBE_API_URL,
      { data: [{ youtube_id: youtubeId, status: "pending" }] },
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
      throw new Error(`TubeArchivist returned HTTP ${response.status}`);
    }

    await collection.updateOne(
      { _id: item._id },
      {
        $set: {
          status: "SENT",
          processedAt: new Date(),
          updatedAt: new Date(),
          lastError: "",
        },
      }
    );
    console.log(`✅ ${youtubeId} accepted and marked as SENT in MongoDB`);
    return true;
  } catch (error) {
    const errorMessage = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message || String(error);

    if (errorMessage.toLowerCase().includes("already")) {
      try {
        await collection.updateOne(
          { _id: item._id },
          {
            $set: {
              status: "SENT",
              processedAt: new Date(),
              updatedAt: new Date(),
              lastError: "",
            },
          }
        );
        console.warn(`⚠️ ${youtubeId} already exists in TubeArchivist; marked SENT`);
        return true;
      } catch (dbError) {
        logError(`Could not mark MongoDB item ${youtubeId} as SENT`, dbError);
        return false;
      }
    }

    logError(`Failed to process MongoDB item ${youtubeId}`, error);
    try {
      const retryCount = (item.retryCount || 0) + 1;
      await collection.updateOne(
        { _id: item._id },
        {
          $set: {
            retryCount,
            lastError: errorMessage,
            status: retryCount >= (item.maxRetries || MAX_ATTEMPTS) ? "FAILED" : "PENDING",
            updatedAt: new Date(),
          },
        }
      );
    } catch (dbError) {
      logError(`Could not save retry information for MongoDB item ${youtubeId}`, dbError);
    }
    return false;
  }
}

async function processMongoQueue() {
  if (!MONGODB_URI) return;

  try {
    const collection = await getMongoQueue();
    const items = await collection
      .find({ status: "PENDING", youtubeId: { $type: "string", $ne: "" } })
      .sort({ createdAt: 1 })
      .limit(BATCH_SIZE)
      .toArray();

    if (items.length === 0) {
      console.log("📭 No pending MongoDB items");
      return;
    }
    console.log(`📋 Found ${items.length} MongoDB queue item(s)`);
    for (const item of items) {
      await processMongoItem(collection, item);
      await delay(REQUEST_DELAY_MS);
    }
  } catch (error) {
    logError("Could not process MongoDB queue", error);
    // Allow a later loop to reconnect after transient network failures.
    if (mongoClient) {
      await mongoClient.close().catch(() => {});
      mongoClient = null;
    }
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

// ---------- TubeArchivist channel subscriptions ----------

function getChannelApiUrl(channelId) {
  const baseUrl = new URL(TUBE_API_URL);
  return new URL(`/api/channel/${encodeURIComponent(channelId)}/`, baseUrl).toString();
}

async function waitForSubscriptionState(item) {
  const expectedState = item.action === "subscribe";
  const deadline = Date.now() + SUBSCRIPTION_VERIFY_TIMEOUT_MS;
  let lastError;

  while (Date.now() < deadline) {
    try {
      const response = await axios.get(getChannelApiUrl(item.channel_id), {
        headers: { Authorization: `Token ${API_TOKEN}`, Accept: "application/json" },
        timeout: 15000,
        validateStatus: (status) => status === 200 || status === 404,
      });

      if (response.status === 200 && response.data?.channel_subscribed === expectedState) {
        return;
      }
      if (response.status === 404 && !expectedState) {
        // An absent channel is already unsubscribed.
        return;
      }
      lastError = new Error(`TubeArchivist has not applied ${item.action} yet`);
    } catch (error) {
      lastError = error;
    }
    await delay(SUBSCRIPTION_VERIFY_INTERVAL_MS);
  }

  throw lastError || new Error(`Timed out waiting for channel ${item.channel_id} to ${item.action}`);
}

async function processSubscriptionItem(item) {
  const subscribe = item.action === "subscribe";
  console.log(`\n📡 ${item.action} channel ${item.channel_id} for chat ${item.chat_id}`);

  try {
    if (subscribe) {
      await axios.post(
        new URL("/api/channel/", TUBE_API_URL).toString(),
        { data: [{ channel_id: item.channel_id, channel_subscribed: true }] },
        {
          headers: {
            Authorization: `Token ${API_TOKEN}`,
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          timeout: 30000,
        }
      );
    } else {
      const response = await axios.post(
        getChannelApiUrl(item.channel_id),
        { channel_subscribed: false },
        {
          headers: {
            Authorization: `Token ${API_TOKEN}`,
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          timeout: 30000,
          validateStatus: (status) => (status >= 200 && status < 300) || status === 404,
        }
      );
      if (response.status === 404) {
        // TubeArchivist has no indexed channel to unsubscribe.
        await pool.query(
          `DELETE FROM public.youtube_channel_subscriptions
           WHERE chat_id = $1 AND channel_id = $2`,
          [item.chat_id, item.channel_id]
        );
        console.log(`✅ Channel ${item.channel_id} is absent; removed unsubscribe request`);
        return;
      }
    }

    // TubeArchivist queues subscribe jobs asynchronously, so only remove the
    // Supabase request after its channel state reflects the requested action.
    await waitForSubscriptionState(item);
    await pool.query(
      `DELETE FROM public.youtube_channel_subscriptions
       WHERE chat_id = $1 AND channel_id = $2`,
      [item.chat_id, item.channel_id]
    );
    console.log(`✅ ${item.action} completed; removed subscription request ${item.channel_id}`);
  } catch (error) {
    logError(`Could not ${item.action} channel ${item.channel_id}`, error);
    await pool.query(
      `UPDATE public.youtube_channel_subscriptions
       SET status = 'pending'
       WHERE chat_id = $1 AND channel_id = $2`,
      [item.chat_id, item.channel_id]
    ).catch((dbError) => logError(`Could not reset subscription request ${item.channel_id}`, dbError));
  }
}

async function processSubscriptionQueue() {
  const result = await pool.query(
    `WITH claimed AS (
       SELECT chat_id, channel_id
       FROM public.youtube_channel_subscriptions
       WHERE status = 'pending'
       ORDER BY created_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE public.youtube_channel_subscriptions AS q
     SET status = 'processing'
     FROM claimed
     WHERE q.chat_id = claimed.chat_id AND q.channel_id = claimed.channel_id
     RETURNING q.chat_id, q.channel_id, q.channel_url, q.action, q.status`,
    [BATCH_SIZE]
  );

  if (result.rows.length === 0) {
    console.log("📭 No pending channel subscription requests");
    return;
  }

  console.log(`📋 Found ${result.rows.length} channel subscription request(s)`);
  for (const item of result.rows) {
    if (!["subscribe", "unsubscribe"].includes(item.action)) {
      console.error(`Unsupported subscription action '${item.action}' for ${item.channel_id}; request left in queue`);
      await pool.query(
        `UPDATE public.youtube_channel_subscriptions SET status = 'failed'
         WHERE chat_id = $1 AND channel_id = $2`,
        [item.chat_id, item.channel_id]
      );
      continue;
    }
    await processSubscriptionItem(item);
    await delay(REQUEST_DELAY_MS);
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

      await runTask("Process channel subscription queue", () => processSubscriptionQueue());

      // MongoDB work is independent; failures do not interrupt Supabase processing.
      await processMongoQueue();

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
