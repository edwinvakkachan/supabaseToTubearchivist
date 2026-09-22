import "dotenv/config";
import pkg from "pg";
const { Pool } = pkg;

if (!process.env.DATABASE_URL) {
  throw new Error("Missing DATABASE_URL. Check your .env file or environment variables.");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

export default pool;
