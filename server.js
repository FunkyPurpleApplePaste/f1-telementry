import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import pg from "pg";
import dotenv from "dotenv";

dotenv.config();

const { Pool } = pg;

const app = express();
const port = process.env.PORT || 3001;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS players (
      id SERIAL PRIMARY KEY,
      name TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id SERIAL PRIMARY KEY,
      player_id INTEGER REFERENCES players(id),
      track TEXT,
      car TEXT,
      started_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS telemetry_packets (
      id SERIAL PRIMARY KEY,
      session_id INTEGER REFERENCES sessions(id),
      packet_type TEXT,
      payload JSONB
    );

    CREATE TABLE IF NOT EXISTS lap_records (
      id SERIAL PRIMARY KEY,
      session_id INTEGER REFERENCES sessions(id),
      lap_number INTEGER,
      lap_time_ms INTEGER
    );
  `);
}

// health
app.get("/health", (req, res) => {
  res.json({ ok: true });
});

// create player
app.post("/players", async (req, res) => {
  const { name } = req.body;
  const r = await pool.query(
    "INSERT INTO players (name) VALUES ($1) RETURNING *",
    [name]
  );
  res.json(r.rows[0]);
});

// create session
app.post("/sessions", async (req, res) => {
  const { playerId, track, car } = req.body;
  const r = await pool.query(
    "INSERT INTO sessions (player_id, track, car) VALUES ($1,$2,$3) RETURNING *",
    [playerId, track, car]
  );
  res.json(r.rows[0]);
});

// add lap
app.post("/laps", async (req, res) => {
  const { sessionId, lapNumber, lapTimeMs } = req.body;
  const r = await pool.query(
    "INSERT INTO lap_records (session_id, lap_number, lap_time_ms) VALUES ($1,$2,$3) RETURNING *",
    [sessionId, lapNumber, lapTimeMs]
  );
  res.json(r.rows[0]);
});

// leaderboard
app.get("/leaderboard", async (req, res) => {
  const r = await pool.query(`
    SELECT p.name, MIN(l.lap_time_ms) AS best
    FROM lap_records l
    JOIN sessions s ON s.id = l.session_id
    JOIN players p ON p.id = s.player_id
    GROUP BY p.name
    ORDER BY best ASC
    LIMIT 10
  `);
  res.json(r.rows);
});

async function start() {
  await initDb();
  app.listen(port, () => console.log("RUNNING", port));
}

start();