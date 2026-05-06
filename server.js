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

  for (const key of ["createdAt", "startedAt", "endedAt", "receivedAt", "recordedAt", "latestTelemetryAt"]) {
    if (out[key] && typeof out[key].toDate === "function") {
      out[key] = out[key].toDate().toISOString();
    }
  }

  return out;
}

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

function mergeProcessedSummary(existing = {}, incoming = {}, latestLap = null) {
  const out = {
    totalSamples: parseInteger(existing.totalSamples, 0) || 0,
    totalLaps: parseInteger(existing.totalLaps, 0) || 0,
    topSpeedKph: parseNumber(existing.topSpeedKph, 0) || 0,
    longestBrakingZoneMeters: parseNumber(existing.longestBrakingZoneMeters, 0) || 0,
    fastestLap: existing.fastestLap ?? null,
    currentLapNumber: existing.currentLapNumber ?? null,
    bestLapTimeMs: existing.bestLapTimeMs ?? null,
    lastLapTimeMs: existing.lastLapTimeMs ?? null,
  };

  const incSamples = parseInteger(incoming.totalSamples, 0) || 0;
  out.totalSamples += incSamples;

  if (incoming.topSpeedKph != null && incoming.topSpeedKph > out.topSpeedKph) {
    out.topSpeedKph = incoming.topSpeedKph;
  }

  if (incoming.longestBrakingZoneMeters != null && incoming.longestBrakingZoneMeters > out.longestBrakingZoneMeters) {
    out.longestBrakingZoneMeters = incoming.longestBrakingZoneMeters;
  }

  if (incoming.currentLapNumber != null) {
    out.currentLapNumber = incoming.currentLapNumber;
  }

  if (incoming.bestLapTimeMs != null) {
    if (out.bestLapTimeMs == null || incoming.bestLapTimeMs < out.bestLapTimeMs) {
      out.bestLapTimeMs = incoming.bestLapTimeMs;
    }
  }

  if (latestLap && latestLap.lapNumber != null) {
    out.lastLapTimeMs = latestLap.lapTimeMs ?? out.lastLapTimeMs;
    out.totalLaps = Math.max(out.totalLaps, latestLap.lapNumber);
    if (latestLap.lapTimeMs != null) {
      if (!out.fastestLap || latestLap.lapTimeMs < out.fastestLap.rawMs) {
        out.fastestLap = {
          lapNumber: latestLap.lapNumber,
          rawMs: latestLap.lapTimeMs,
          formattedTime: formatLapTime(latestLap.lapTimeMs),
        };
      }
    }
  }

  return out;
}

async function applySessionSummary(sessionRef, patch, latestLap = null) {
  const snap = await sessionRef.get();
  if (!snap.exists) return;

  const current = snap.data() || {};
  const mergedSummary = mergeProcessedSummary(current.processedSummary || {}, patch, latestLap);

  const updateBody = {
    latestTelemetry: patch.latestTelemetry ?? current.latestTelemetry ?? null,
    latestTelemetryAt: FieldValue.serverTimestamp(),
    processedSummary: mergedSummary,
  };

  await sessionRef.set(updateBody, { merge: true });
}

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/players", async (req, res) => {
  try {
    const name = safeString(req.body.name);
    if (!name) return res.status(400).json({ error: "name is required" });

    const ref = db.collection("players").doc(); // new doc every time

    await ref.set({
      name,
      createdAt: FieldValue.serverTimestamp(),
      lastSeenAt: FieldValue.serverTimestamp()
    });

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
    const playerName = safeString(req.body.playerName, null);
    const trackName = safeString(req.body.trackName, null);
    const trackId = parseInteger(req.body.trackId, null);
    const sessionType = parseInteger(req.body.sessionType, null);

    if (!playerId) return res.status(400).json({ error: "playerId is required" });

    const sessionRef = db.collection("sessions").doc();
    await sessionRef.set({
      playerId,
      playerName,
      trackName,
      trackId,
      sessionType,
      startedAt: FieldValue.serverTimestamp(),
      endedAt: null,
      latestTelemetry: null,
      latestTelemetryAt: null,
      processedSummary: {
        totalSamples: 0,
        totalLaps: 0,
        topSpeedKph: 0,
        longestBrakingZoneMeters: 0,
        fastestLap: null,
        currentLapNumber: null,
        bestLapTimeMs: null,
        lastLapTimeMs: null
      }
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

    await sessionRef.update({ endedAt: FieldValue.serverTimestamp() });

    const updated = await sessionRef.get();
    res.json(serializeDoc(updated));
  } catch (err) {
    console.error("POST /sessions/:id/end error:", err);
    res.status(500).json({ error: "failed to end session" });
  }
});

app.post("/sessions/:id/laps", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const lapNumber = parseInteger(req.body.lapNumber);
    const lapTimeMs = parseInteger(req.body.lapTimeMs);
    const trackName = safeString(req.body.trackName, null);
    const trackId = parseInteger(req.body.trackId, null);

    if (!sessionId || lapNumber === null) {
      return res.status(400).json({ error: "sessionId and lapNumber required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const lapRef = sessionRef.collection("laps").doc(`lap_${lapNumber}`);
    const lapData = {
      lapNumber,
      lapTimeMs,
      trackName,
      trackId,
      recordedAt: FieldValue.serverTimestamp()
    };

    await lapRef.set(lapData);

    const currentSummary = sessionSnap.data()?.processedSummary || {};
    const nextSummary = mergeProcessedSummary(currentSummary, {}, {
      lapNumber,
      lapTimeMs
    });

    await sessionRef.set(
      {
        processedSummary: nextSummary
      },
      { merge: true }
    );

    res.status(201).json({ success: true, lapNumber });
  } catch (err) {
    console.error("POST /sessions/:id/laps error:", err);
    res.status(500).json({ error: "failed to save lap" });
  }
});

app.post("/telemetry/latest", async (req, res) => {
  try {
    const sessionId = safeString(req.body.sessionId);
    const latestTelemetry = req.body.latestTelemetry;

    if (!sessionId || !latestTelemetry) {
      return res.status(400).json({ error: "sessionId and latestTelemetry required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    await sessionRef.set(
      {
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp()
      },
      { merge: true }
    );

    res.json({ success: true });
  } catch (err) {
    console.error("POST /telemetry/latest error:", err);
    res.status(500).json({ error: "failed to update latest telemetry" });
  }
});

app.post("/telemetry/batch", async (req, res) => {
  try {
    const sessionId = safeString(req.body.sessionId);
    const samples = req.body.samples;

    if (!sessionId) return res.status(400).json({ error: "sessionId is required" });
    if (!Array.isArray(samples) || samples.length === 0) {
      return res.status(400).json({ error: "samples array is required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const chunkRef = sessionRef.collection("telemetryChunks").doc();
    const latestTelemetry = samples[samples.length - 1];

    let maxSpeed = 0;
    let longestBrakingZoneMeters = 0;
    let currentLapNumber = null;
    let bestLapTimeMs = sessionSnap.data()?.processedSummary?.bestLapTimeMs ?? null;
    let fastestLap = sessionSnap.data()?.processedSummary?.fastestLap ?? null;

    for (const s of samples) {
      if (s?.speedKph != null && s.speedKph > maxSpeed) {
        maxSpeed = s.speedKph;
      }
      if (s?.brakingDistance != null && s.brakingDistance > longestBrakingZoneMeters) {
        longestBrakingZoneMeters = s.brakingDistance;
      }
      if (s?.lapNumber != null) {
        currentLapNumber = s.lapNumber;
      }
      if (s?.deltaToPB != null) {
        const candidate = parseInteger(s.deltaToPB, null);
        if (candidate != null) {
          if (bestLapTimeMs == null || candidate < bestLapTimeMs) {
            bestLapTimeMs = candidate;
          }
        }
      }
    }

    await chunkRef.set({
      samples,
      count: samples.length,
      receivedAt: FieldValue.serverTimestamp()
    });

    const currentSummary = sessionSnap.data()?.processedSummary || {};
    const mergedSummary = {
      totalSamples: (parseInteger(currentSummary.totalSamples, 0) || 0) + samples.length,
      totalLaps: currentSummary.totalLaps || 0,
      topSpeedKph: Math.max(parseNumber(currentSummary.topSpeedKph, 0) || 0, maxSpeed),
      longestBrakingZoneMeters: Math.max(parseNumber(currentSummary.longestBrakingZoneMeters, 0) || 0, longestBrakingZoneMeters),
      fastestLap: fastestLap,
      currentLapNumber: currentLapNumber ?? currentSummary.currentLapNumber ?? null,
      bestLapTimeMs: bestLapTimeMs ?? currentSummary.bestLapTimeMs ?? null,
      lastLapTimeMs: currentSummary.lastLapTimeMs ?? null,
    };

    await sessionRef.set(
      {
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp(),
        processedSummary: mergedSummary
      },
      { merge: true }
    );

    res.status(201).json({ success: true, count: samples.length });
  } catch (err) {
    console.error("POST /telemetry/batch error:", err);
    res.status(500).json({ error: "failed to save telemetry batch" });
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
    strategy: "Session doc holds latestTelemetry + processedSummary"
  });
});

async function start() {
  app.listen(port, () => console.log("API RUNNING on port", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});