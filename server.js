import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import dotenv from "dotenv";
import fs from "fs";

import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

let credential;
if (process.env.GOOGLE_APPLICATION_CREDENTIALS && fs.existsSync(process.env.GOOGLE_APPLICATION_CREDENTIALS)) {
  const serviceAccount = JSON.parse(
    fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, "utf8")
  );
  credential = cert(serviceAccount);
} else {
  // Works in Google-hosted environments that provide ADC
  credential = applicationDefault();
}

initializeApp({ credential });
const db = getFirestore();

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use(morgan("dev"));

function parseNumber(value, fallback = null) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseInteger(value, fallback = null) {
  const n = parseNumber(value, fallback);
  if (n === null) return fallback;
  return Math.trunc(n);
}

function safeString(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  const s = String(value).trim();
  return s.length ? s : fallback;
}

function playerDocIdFromName(name) {
  return String(name)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function serializeDoc(docSnap) {
  const data = docSnap.data();
  const out = { id: docSnap.id, ...data };

  for (const key of ["createdAt", "startedAt", "endedAt", "receivedAt", "timestamp"]) {
    if (out[key] && typeof out[key].toDate === "function") {
      out[key] = out[key].toDate().toISOString();
    }
  }

  return out;
}

async function initDb() {
  // Firestore does not need table creation.
  // Collections/documents appear automatically on first write.
  await Promise.resolve();
}

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/players", async (req, res) => {
  try {
    const name = safeString(req.body.name);
    if (!name) {
      return res.status(400).json({ error: "name is required" });
    }

    const playerId = playerDocIdFromName(name);
    if (!playerId) {
      return res.status(400).json({ error: "invalid player name" });
    }

    const ref = db.collection("players").doc(playerId);

    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (snap.exists) {
        throw new Error("PLAYER_EXISTS");
      }
      tx.set(ref, {
        name,
        createdAt: FieldValue.serverTimestamp()
      });
    });

    const saved = await ref.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    if (err.message === "PLAYER_EXISTS") {
      return res.status(409).json({ error: "player already exists" });
    }
    console.error("POST /players error:", err);
    res.status(500).json({ error: "failed to create player" });
  }
});

app.get("/players", async (req, res) => {
  try {
    const snap = await db.collection("players").orderBy("createdAt", "desc").get();
    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /players error:", err);
    res.status(500).json({ error: "failed to fetch players" });
  }
});

app.post("/sessions", async (req, res) => {
  try {
    const playerId = safeString(req.body.playerId);
    const track = safeString(req.body.track, null);
    const car = safeString(req.body.car, null);
    const gameVersion = safeString(req.body.gameVersion, null);
    const notes = safeString(req.body.notes, null);

    if (!playerId) {
      return res.status(400).json({ error: "playerId is required" });
    }

    const playerRef = db.collection("players").doc(playerId);
    const playerSnap = await playerRef.get();

    if (!playerSnap.exists) {
      return res.status(404).json({ error: "player not found" });
    }

    const sessionRef = db.collection("sessions").doc();
    await sessionRef.set({
      playerId,
      track,
      car,
      gameVersion,
      notes,
      startedAt: FieldValue.serverTimestamp(),
      endedAt: null
    });

    const saved = await sessionRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /sessions error:", err);
    res.status(500).json({ error: "failed to create session" });
  }
});

app.post("/sessions/:id/end", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    await sessionRef.update({
      endedAt: FieldValue.serverTimestamp()
    });

    const updated = await sessionRef.get();
    res.json(serializeDoc(updated));
  } catch (err) {
    console.error("POST /sessions/:id/end error:", err);
    res.status(500).json({ error: "failed to end session" });
  }
});

app.get("/sessions/:id", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    if (!sessionId) {
      return res.status(400).json({ error: "invalid session id" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const [packetsSnap, samplesSnap] = await Promise.all([
      sessionRef
        .collection("telemetryPackets")
        .orderBy("receivedAt", "asc")
        .limit(500)
        .get(),
      sessionRef
        .collection("telemetrySamples")
        .orderBy("receivedAt", "asc")
        .limit(5000)
        .get()
    ]);

    res.json({
      session: serializeDoc(sessionSnap),
      packets: packetsSnap.docs.map(serializeDoc),
      samples: samplesSnap.docs.map(serializeDoc)
    });
  } catch (err) {
    console.error("GET /sessions/:id error:", err);
    res.status(500).json({ error: "failed to fetch session" });
  }
});

app.post("/telemetry/packet", async (req, res) => {
  try {
    const sessionId = safeString(req.body.sessionId);
    const packetType = safeString(req.body.packetType);
    const packetIndex = parseInteger(req.body.packetIndex);
    const gameTimeMs = parseInteger(req.body.gameTimeMs);
    const payload = req.body.payload;

    if (!sessionId || !packetType || payload === undefined) {
      return res.status(400).json({
        error: "sessionId, packetType, and payload are required"
      });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const packetRef = sessionRef.collection("telemetryPackets").doc();
    await packetRef.set({
      packetIndex,
      packetType,
      gameTimeMs,
      payload,
      receivedAt: FieldValue.serverTimestamp()
    });

    const saved = await packetRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /telemetry/packet error:", err);
    res.status(500).json({ error: "failed to save telemetry packet" });
  }
});

app.post("/telemetry/sample", async (req, res) => {
  try {
    const sessionId = safeString(req.body.sessionId);
    const sampleIndex = parseInteger(req.body.sampleIndex);
    const timestamp = req.body.timestamp ? new Date(req.body.timestamp) : null;

    const speedKph = parseInteger(req.body.speedKph);
    const throttle = parseNumber(req.body.throttle);
    const brake = parseNumber(req.body.brake);
    const steer = parseNumber(req.body.steer);
    const gear = parseInteger(req.body.gear);
    const rpm = parseInteger(req.body.rpm);
    const drs = parseInteger(req.body.drs);
    const playerCarIndex = parseInteger(req.body.playerCarIndex);
    const payload = req.body.payload;

    if (!sessionId) {
      return res.status(400).json({ error: "sessionId is required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const sampleRef = sessionRef.collection("telemetrySamples").doc();
    await sampleRef.set({
      sampleIndex,
      timestamp: timestamp && !Number.isNaN(timestamp.getTime()) ? timestamp : FieldValue.serverTimestamp(),
      speedKph,
      throttle,
      brake,
      steer,
      gear,
      rpm,
      drs,
      playerCarIndex,
      payload: payload ?? null,
      receivedAt: FieldValue.serverTimestamp()
    });

    const saved = await sampleRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /telemetry/sample error:", err);
    res.status(500).json({ error: "failed to save telemetry sample" });
  }
});

app.get("/schema", (req, res) => {
  res.json({
    collections: [
      "players",
      "sessions",
      "sessions/{sessionId}/telemetryPackets",
      "sessions/{sessionId}/telemetrySamples"
    ],
    strategy: "Firestore documents + subcollections"
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