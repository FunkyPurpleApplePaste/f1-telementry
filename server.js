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
app.use(express.json({ limit: "2mb" }));
app.use(morgan("dev"));

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS players (
      id SERIAL PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id SERIAL PRIMARY KEY,
      player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
      track TEXT,
      car TEXT,
      game_version TEXT,
      notes TEXT,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at TIMESTAMPTZ
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS telemetry_packets (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      packet_index INTEGER,
      packet_type TEXT NOT NULL,
      game_time_ms INTEGER,
      payload JSONB NOT NULL,
      received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/players", async (req, res) => {
  try {
    const { name } = req.body;
    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: "name is required" });
    }

    const result = await pool.query(
      `INSERT INTO players (name)
       VALUES ($1)
       RETURNING *`,
      [String(name).trim()]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    if (String(err.message).toLowerCase().includes("unique")) {
      return res.status(409).json({ error: "player already exists" });
    }
    console.error("POST /players error:", err);
    res.status(500).json({ error: "failed to create player" });
  }
});

app.get("/players", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM players ORDER BY created_at DESC`
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET /players error:", err);
    res.status(500).json({ error: "failed to fetch players" });
  }
});

app.post("/sessions", async (req, res) => {
  try {
    const { playerId, track, car, gameVersion, notes } = req.body;

    if (!playerId) {
      return res.status(400).json({ error: "playerId is required" });
    }

    const result = await pool.query(
      `INSERT INTO sessions (player_id, track, car, game_version, notes)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [
        Number(playerId),
        track ?? null,
        car ?? null,
        gameVersion ?? null,
        notes ?? null
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("POST /sessions error:", err);
    res.status(500).json({ error: "failed to create session" });
  }
});

app.get("/sessions/:id", async (req, res) => {
  try {
    const sessionId = Number(req.params.id);
    if (!Number.isInteger(sessionId) || sessionId <= 0) {
      return res.status(400).json({ error: "invalid session id" });
    }

    const sessionResult = await pool.query(
      `SELECT s.*, p.name AS player_name
       FROM sessions s
       JOIN players p ON p.id = s.player_id
       WHERE s.id = $1`,
      [sessionId]
    );

    if (sessionResult.rowCount === 0) {
      return res.status(404).json({ error: "session not found" });
    }

    const packetsResult = await pool.query(
      `SELECT *
       FROM telemetry_packets
       WHERE session_id = $1
       ORDER BY received_at ASC, id ASC
       LIMIT 500`,
      [sessionId]
    );

    res.json({
      session: sessionResult.rows[0],
      packets: packetsResult.rows
    });
  } catch (err) {
    console.error("GET /sessions/:id error:", err);
    res.status(500).json({ error: "failed to fetch session" });
  }
});

app.post("/telemetry/packet", async (req, res) => {
  try {
    const {
      sessionId,
      packetType,
      packetIndex,
      gameTimeMs,
      payload
    } = req.body;

    if (!sessionId || !packetType || payload === undefined) {
      return res.status(400).json({
        error: "sessionId, packetType, and payload are required"
      });
    }

    const result = await pool.query(
      `INSERT INTO telemetry_packets (
         session_id, packet_index, packet_type, game_time_ms, payload
       )
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [
        Number(sessionId),
        packetIndex ?? null,
        String(packetType),
        gameTimeMs ?? null,
        JSON.stringify(payload)
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("POST /telemetry/packet error:", err);
    res.status(500).json({ error: "failed to save telemetry packet" });
  }
});

app.get("/schema", (req, res) => {
  res.json({
    tables: ["players", "sessions", "telemetry_packets"],
    strategy: "store raw packets first, parse into useful fields later"
  });
});

async function start() {
  await initDb();
  app.listen(port, () => console.log("RUNNING", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});