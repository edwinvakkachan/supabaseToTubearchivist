import pkg from "pg";
// const { Pool } = pkg;
import 'dotenv/config';
import pool from "./pool.js";



export async function initDB() {
 
console.log('supabase db connected');

  return pool;
}

