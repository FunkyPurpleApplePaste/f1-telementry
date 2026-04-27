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

function isValidId(value) {
  return Number.isInteger(Number(value)) && Number(value) > 0;
}

function toInt(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number.parseInt(value, 10);
  return Number.isNaN(n) ? null : n;
}

function toNum(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}

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
      lap_number INTEGER,
      game_time_ms INTEGER,
      payload JSONB NOT NULL,
      received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS telemetry_metrics (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      packet_id INTEGER REFERENCES telemetry_packets(id) ON DELETE SET NULL,
      lap_number INTEGER,
      metric_key TEXT NOT NULL,
      metric_value_num DOUBLE PRECISION,
      metric_value_text TEXT,
      metric_value_bool BOOLEAN,
      metric_value_json JSONB,
      source TEXT,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS lap_records (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      lap_number INTEGER NOT NULL,
      lap_time_ms INTEGER NOT NULL,
      sector1_ms INTEGER,
      sector2_ms INTEGER,
      sector3_ms INTEGER,
      valid BOOLEAN NOT NULL DEFAULT TRUE,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(session_id, lap_number)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sessions_player_id ON sessions(player_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_packets_session_id ON telemetry_packets(session_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_metrics_session_id ON telemetry_metrics(session_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_laps_session_id ON lap_records(session_id);
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

    if (!isValidId(playerId)) {
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
    const sessionId = toInt(req.params.id);
    if (!sessionId) {
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

    const lapsResult = await pool.query(
      `SELECT *
       FROM lap_records
       WHERE session_id = $1
       ORDER BY lap_number ASC`,
      [sessionId]
    );

    const packetsResult = await pool.query(
      `SELECT *
       FROM telemetry_packets
       WHERE session_id = $1
       ORDER BY received_at ASC, id ASC
       LIMIT 200`,
      [sessionId]
    );

    const metricsResult = await pool.query(
      `SELECT *
       FROM telemetry_metrics
       WHERE session_id = $1
       ORDER BY recorded_at ASC, id ASC
       LIMIT 500`,
      [sessionId]
    );

    res.json({
      session: sessionResult.rows[0],
      laps: lapsResult.rows,
      packets: packetsResult.rows,
      metrics: metricsResult.rows
    });
  } catch (err) {
    console.error("GET /sessions/:id error:", err);
    res.status(500).json({ error: "failed to fetch session" });
  }
});

app.post("/laps", async (req, res) => {
  try {
    const {
      sessionId,
      lapNumber,
      lapTimeMs,
      sector1Ms,
      sector2Ms,
      sector3Ms,
      valid = true
    } = req.body;

    if (!isValidId(sessionId) || !isValidId(lapNumber) || !isValidId(lapTimeMs)) {
      return res.status(400).json({
        error: "sessionId, lapNumber, and lapTimeMs are required"
      });
    }

    const result = await pool.query(
      `INSERT INTO lap_records (
         session_id, lap_number, lap_time_ms,
         sector1_ms, sector2_ms, sector3_ms, valid
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (session_id, lap_number)
       DO UPDATE SET
         lap_time_ms = EXCLUDED.lap_time_ms,
         sector1_ms = EXCLUDED.sector1_ms,
         sector2_ms = EXCLUDED.sector2_ms,
         sector3_ms = EXCLUDED.sector3_ms,
         valid = EXCLUDED.valid,
         recorded_at = NOW()
       RETURNING *`,
      [
        Number(sessionId),
        Number(lapNumber),
        Number(lapTimeMs),
        toInt(sector1Ms),
        toInt(sector2Ms),
        toInt(sector3Ms),
        Boolean(valid)
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("POST /laps error:", err);
    res.status(500).json({ error: "failed to save lap" });
  }
});

app.post("/telemetry/packet", async (req, res) => {
  try {
    const {
      sessionId,
      packetType,
      packetIndex,
      lapNumber,
      gameTimeMs,
      payload
    } = req.body;

    if (!isValidId(sessionId) || !packetType || payload === undefined) {
      return res.status(400).json({
        error: "sessionId, packetType, and payload are required"
      });
    }

    const result = await pool.query(
      `INSERT INTO telemetry_packets (
         session_id, packet_index, packet_type, lap_number, game_time_ms, payload
       )
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [
        Number(sessionId),
        toInt(packetIndex),
        String(packetType),
        toInt(lapNumber),
        toInt(gameTimeMs),
        JSON.stringify(payload)
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("POST /telemetry/packet error:", err);
    res.status(500).json({ error: "failed to save telemetry packet" });
  }
});

app.post("/telemetry/metric", async (req, res) => {
  try {
    const {
      sessionId,
      packetId,
      lapNumber,
      metricKey,
      metricValueNum,
      metricValueText,
      metricValueBool,
      metricValueJson,
      source
    } = req.body;

    if (!isValidId(sessionId) || !metricKey) {
      return res.status(400).json({
        error: "sessionId and metricKey are required"
      });
    }

    const result = await pool.query(
      `INSERT INTO telemetry_metrics (
         session_id, packet_id, lap_number, metric_key,
         metric_value_num, metric_value_text, metric_value_bool,
         metric_value_json, source
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING *`,
      [
        Number(sessionId),
        toInt(packetId),
        toInt(lapNumber),
        String(metricKey),
        metricValueNum ?? null,
        metricValueText ?? null,
        typeof metricValueBool === "boolean" ? metricValueBool : null,
        metricValueJson ?? null,
        source ?? "manual"
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("POST /telemetry/metric error:", err);
    res.status(500).json({ error: "failed to save metric" });
  }
});

app.get("/leaderboard", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        p.name,
        MIN(l.lap_time_ms) AS best_lap_ms
      FROM lap_records l
      JOIN sessions s ON s.id = l.session_id
      JOIN players p ON p.id = s.player_id
      WHERE l.valid = TRUE
      GROUP BY p.name
      ORDER BY best_lap_ms ASC
      LIMIT 10
    `);

    res.json(result.rows);
  } catch (err) {
    console.error("GET /leaderboard error:", err);
    res.status(500).json({ error: "failed to fetch leaderboard" });
  }
});

app.get("/schema", async (req, res) => {
  res.json({
    tables: [
      "players",
      "sessions",
      "telemetry_packets",
      "telemetry_metrics",
      "lap_records"
    ],
    note: "telemetry_packets stores raw data; telemetry_metrics stores anything parsed later."
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