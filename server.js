import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import dotenv from "dotenv";

import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

let credential;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  credential = cert(serviceAccount);
} else {
  credential = applicationDefault();
}

initializeApp({ credential });
const db = getFirestore();

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: "5mb" }));
app.use(morgan("dev"));

// --- Helper Functions ---
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
  return String(name).trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "");
}

function serializeDoc(docSnap) {
  const data = docSnap.data();
  const out = { id: docSnap.id, ...data };

  for (const key of ["createdAt", "startedAt", "endedAt", "receivedAt", "recordedAt", "latestTelemetryAt"]) {
    if (out[key] && typeof out[key].toDate === "function") {
      out[key] = out[key].toDate().toISOString();
    }
  }
  return out;
}

app.post("/telemetry/latest", async (req, res) => {
  try {
    const { sessionId, latestTelemetry } = req.body;

    if (!sessionId || !latestTelemetry) {
      return res.status(400).json({ error: "sessionId and latestTelemetry required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);

    await sessionRef.update({
      latestTelemetry,
      latestUpdatedAt: FieldValue.serverTimestamp()
    });

    res.json({ success: true });
  } catch (err) {
    console.error("latestTelemetry error:", err);
    res.status(500).json({ error: "failed to update latest telemetry" });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/players", async (req, res) => {
  try {
    const name = safeString(req.body.name);
    if (!name) return res.status(400).json({ error: "name is required" });

    const playerId = playerDocIdFromName(name);
    const ref = db.collection("players").doc(playerId);
    await ref.set({ name, createdAt: FieldValue.serverTimestamp() });

    const saved = await ref.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /players error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/sessions", async (req, res) => {
  try {
    const snap = await db.collection("sessions")
      .orderBy("startedAt", "desc")
      .limit(20)
      .get();

    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /sessions error:", err);
    res.status(500).json({ error: "failed to fetch sessions" });
  }
});

app.post("/sessions", async (req, res) => {
  try {
    const playerId = safeString(req.body.playerId);
    if (!playerId) return res.status(400).json({ error: "playerId is required" });

    const sessionRef = db.collection("sessions").doc();
    await sessionRef.set({
      playerId,
      startedAt: FieldValue.serverTimestamp(),
      endedAt: null,
      latestTelemetry: null,
      latestTelemetryAt: null
    });

    const saved = await sessionRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    res.status(500).json({ error: "failed to create session" });
  }
});

app.post("/sessions/:id/end", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    await sessionRef.update({ endedAt: FieldValue.serverTimestamp() });

    const updated = await sessionRef.get();
    res.json(serializeDoc(updated));
  } catch (err) {
    res.status(500).json({ error: "failed to end session" });
  }
});

app.post("/sessions/:id/laps", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const lapNumber = parseInteger(req.body.lapNumber);
    const lapTimeMs = parseInteger(req.body.lapTimeMs);

    if (!sessionId || lapNumber === null) {
      return res.status(400).json({ error: "sessionId and lapNumber required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const lapRef = sessionRef.collection("laps").doc(`lap_${lapNumber}`);

    await lapRef.set({
      lapNumber,
      lapTimeMs,
      recordedAt: FieldValue.serverTimestamp()
    });

    res.status(201).json({ success: true, lapNumber });
  } catch (err) {
    console.error("POST /sessions/:id/laps error:", err);
    res.status(500).json({ error: "failed to save lap" });
  }
});

app.post("/telemetry/batch", async (req, res) => {
  try {
    const { sessionId, samples } = req.body;
    if (!sessionId) return res.status(400).json({ error: "sessionId is required" });
    if (!Array.isArray(samples) || samples.length === 0) {
      return res.status(400).json({ error: "samples array is required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const chunkRef = sessionRef.collection("telemetryChunks").doc();

    const latestTelemetry = samples[samples.length - 1];

    await chunkRef.set({
      samples,
      count: samples.length,
      receivedAt: FieldValue.serverTimestamp()
    });

    await sessionRef.set(
      {
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp()
      },
      { merge: true }
    );

    res.status(201).json({ success: true, count: samples.length });
  } catch (err) {
    console.error("POST /telemetry/batch error:", err);
    res.status(500).json({ error: "failed to save telemetry batch" });
  }
});

function formatLapTime(ms) {
  if (!ms) return "--:--.---";
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.floor((ms % 60000) / 1000);
  const fraction = ms % 1000;
  return `${minutes}:${seconds.toString().padStart(2, "0")}.${fraction.toString().padStart(3, "0")}`;
}

function downsample(data, targetCount = 500) {
  if (data.length <= targetCount) return data;
  const step = Math.ceil(data.length / targetCount);
  return data.filter((_, index) => index % step === 0);
}

app.get("/sessions/:id/processed", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "Session not found" });
    }

    const lapsSnap = await sessionRef.collection("laps").orderBy("lapNumber", "asc").get();
    const processedLaps = lapsSnap.docs.map(doc => {
      const data = doc.data();
      return {
        lapNumber: data.lapNumber,
        rawMs: data.lapTimeMs,
        formattedTime: formatLapTime(data.lapTimeMs)
      };
    });

    const chunksSnap = await sessionRef.collection("telemetryChunks").orderBy("receivedAt", "asc").get();

    let allSamples = [];
    chunksSnap.forEach(doc => {
      const chunkData = doc.data();
      if (Array.isArray(chunkData.samples)) {
        allSamples.push(...chunkData.samples);
      }
    });

    allSamples.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    let maxSpeed = 0;
    let maxBrakingDistance = 0;

    allSamples.forEach(sample => {
      if (sample.speedKph > maxSpeed) maxSpeed = sample.speedKph;
      if (sample.brakingDistance > maxBrakingDistance) maxBrakingDistance = sample.brakingDistance;
    });

    const chartData = downsample(allSamples, 500);

    res.json({
      sessionInfo: serializeDoc(sessionSnap),
      insights: {
        totalLaps: processedLaps.length,
        fastestLap: processedLaps.sort((a, b) => a.rawMs - b.rawMs)[0] || null,
        topSpeedKph: maxSpeed,
        longestBrakingZoneMeters: Math.round(maxBrakingDistance * 10) / 10
      },
      laps: processedLaps,
      chartData: chartData
    });
  } catch (err) {
    console.error("GET /sessions/:id/processed error:", err);
    res.status(500).json({ error: "failed to process session data" });
  }
});

app.get("/schema", (req, res) => {
  res.json({
    collections: [
      "players",
      "sessions",
      "sessions/{sessionId}/telemetryChunks",
      "sessions/{sessionId}/laps"
    ],
    strategy: "Firestore documents + subcollections + JSON Array Chunks"
  });
});

async function start() {
  app.listen(port, () => console.log("API RUNNING on port", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});