import dotenv from 'dotenv'
dotenv.config();
import axios from 'axios';
import { initDB } from './supabase/db.js';
import pool from './supabase/pool.js';
import { delay } from './delay.js';


// ---------- Env ----------
const API_TOKEN = process.env.API_TOKEN;
const TUBE_API_URL = process.env.TUBE_API_URL;





// ---------- Main Worker ----------
async function processQueue() {
  console.log('Starting queue worker...');

  while (true) {
    try {
      await initDB();
      
      const { rows: items } = await pool.query(`
    SELECT *
    FROM youtube_queue
    WHERE status = 'pending'
    ORDER BY created_at ASC
    LIMIT 50
`);

      if (!items || items.length === 0) {
         console.log('Queue empty...');
        await delay(60000);
        continue;
      }

       console.log(`Found ${items.length} pending item(s)`);

      for (const item of items) {
        try {
          // Mark processing first
try {
    await pool.query(`
        UPDATE youtube_queue
        SET status = $1
        WHERE id = $2
    `, ['processing', item.id]);
} catch (error) {
    console.error('Error updating queue item:', error);
}

          const payload = {
            data: [
              {
                youtube_id: item.youtube_id,
                status: 'pending',
              },
            ],
          };

           console.log(`Sending ${item.youtube_id} to TubeArchivist...`);

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
            console.log(`TubeArchivist accepted ${item.youtube_id}`);

await pool.query(`
    UPDATE youtube_queue
    SET
        status = 'sent',
        sent_at = NOW(),
        error_message = NULL
    WHERE id = $1
`, [item.id]);


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

await pool.query(`
    UPDATE youtube_queue
    SET
        status = 'sent',
        sent_at = NOW(),
        error_message = NULL
    WHERE id = $1
`, [item.id]);

            continue;
          }

          console.error(`Failed for ${item.youtube_id}:`, errorMessage);

    await pool.query(`
    UPDATE youtube_queue
    SET
        status = 'failed',
        error_message = $1
    WHERE id = $2
`, [errorMessage, item.id]);
        }

        // Small delay between requests
        await delay(3000);
      }

    } catch (e) {
      console.error('Worker loop crashed:', e.message);
      await delay(10000);
    }
  }
}

// ---------- Run ----------
processQueue().catch((e) => {
  console.error('Fatal worker crash:', e);
});