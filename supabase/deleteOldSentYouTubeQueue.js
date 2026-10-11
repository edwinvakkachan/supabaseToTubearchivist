
import pool from "./pool.js";

export async function deleteOldSentYouTubeQueue() {
  console.log("[YouTube Queue Cleanup] Starting cleanup...");

  try {
    const result = await pool.query(`
      DELETE FROM public.youtube_queue
      WHERE status = 'sent'
        AND sent_at <= NOW() - INTERVAL '12 hours'
      RETURNING id, youtube_id, sent_at;
    `);

    console.log(
      `[YouTube Queue Cleanup] Deleted ${result.rowCount} sent rows older than 12 hours.`
    );

    for (const row of result.rows) {
      console.log(
        `[YouTube Queue Cleanup] Deleted ID=${row.id}, ` +
        `YouTube ID=${row.youtube_id}, Sent=${row.sent_at}`
      );
    }

    return result.rowCount;
  } catch (error) {
    console.error(
      "[YouTube Queue Cleanup] Failed:",
      error
    );

    throw error;
  }
}
