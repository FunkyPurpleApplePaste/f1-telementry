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

function normalizeUsername(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeEmail(value) {
  const s = safeString(value, null);
  return s ? s.toLowerCase() : null;
}

function serializeDoc(docSnap) {
  const data = docSnap.data();
  const out = { id: docSnap.id, ...data };

  for (const key of [
    "createdAt",
    "startedAt",
    "endedAt",
    "receivedAt",
    "recordedAt",
    "latestTelemetryAt",
    "lastSeenAt",
  ]) {
    if (out[key] && typeof out[key].toDate === "function") {
      out[key] = out[key].toDate().toISOString();
    }
  }

  return out;
}

function parseBoolean(value, fallback = null) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;

  const s = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(s)) return true;
  if (["false", "0", "no", "n", "off"].includes(s)) return false;
  return fallback;
}

function formatLapTime(ms) {
  if (!ms) return "--:--.---";
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.floor((ms % 60000) / 1000);
  const fraction = ms % 1000;
  return `${minutes}:${seconds.toString().padStart(2, "0")}.${fraction
    .toString()
    .padStart(3, "0")}`;
}

function mergeProcessedSummary(existing = {}, incoming = {}, latestLap = null) {
  const out = {
    totalSamples: parseInteger(existing.totalSamples, 0) || 0,
    totalLaps: parseInteger(existing.totalLaps, 0) || 0,
    topSpeedKph: parseNumber(existing.topSpeedKph, 0) || 0,
    longestBrakingZoneMeters:
      parseNumber(existing.longestBrakingZoneMeters, 0) || 0,
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

  if (
    incoming.longestBrakingZoneMeters != null &&
    incoming.longestBrakingZoneMeters > out.longestBrakingZoneMeters
  ) {
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

async function ensureUserRecord({ username, email = null }) {
  const cleanUsername = safeString(username);
  if (!cleanUsername) throw new Error("username is required");

  const usernameLower = normalizeUsername(cleanUsername);
  const emailValue = safeString(email, null);
  const emailLower = normalizeEmail(emailValue);

  const usernameRef = db.collection("usernames").doc(usernameLower);

  return await db.runTransaction(async (tx) => {
    const mapSnap = await tx.get(usernameRef);

    if (mapSnap.exists) {
      const { userId } = mapSnap.data();
      const userRef = db.collection("users").doc(userId);
      const userSnap = await tx.get(userRef);

      if (!userSnap.exists) {
        throw new Error("username mapping is broken");
      }

      const updateBody = {
        username: cleanUsername,
        usernameLower,
        lastSeenAt: FieldValue.serverTimestamp(),
      };

      if (emailValue) {
        updateBody.email = emailValue;
        updateBody.emailLower = emailLower;
      }

      tx.set(userRef, updateBody, { merge: true });

      const current = userSnap.data() || {};
      return {
        id: userId,
        ...current,
        ...updateBody,
      };
    }

    const userRef = db.collection("users").doc();
    const userData = {
      username: cleanUsername,
      usernameLower,
      email: emailValue,
      emailLower,
      createdAt: FieldValue.serverTimestamp(),
      lastSeenAt: FieldValue.serverTimestamp(),
    };

    tx.set(userRef, userData);
    tx.set(usernameRef, {
      userId: userRef.id,
      usernameLower,
      createdAt: FieldValue.serverTimestamp(),
    });

    return {
      id: userRef.id,
      ...userData,
    };
  });
}

async function applySessionSummary(sessionRef, patch, latestLap = null) {
  const snap = await sessionRef.get();
  if (!snap.exists) return;

  const current = snap.data() || {};
  const mergedSummary = mergeProcessedSummary(
    current.processedSummary || {},
    patch,
    latestLap
  );

  const updateBody = {
    latestTelemetry: patch.latestTelemetry ?? current.latestTelemetry ?? null,
    latestTelemetryAt: FieldValue.serverTimestamp(),
    processedSummary: mergedSummary,
  };

  await sessionRef.set(updateBody, { merge: true });
}

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/users/ensure", async (req, res) => {
  try {
    const user = await ensureUserRecord({
      username: req.body.username,
      email: req.body.email ?? null,
    });
    res.status(201).json(user);
  } catch (err) {
    console.error("POST /users/ensure error:", err);
    res.status(400).json({ error: err.message });
  }
});

app.get("/users/resolve", async (req, res) => {
  try {
    const username = safeString(req.query.username);
    if (!username) {
      return res.status(400).json({ error: "username is required" });
    }

    const usernameLower = normalizeUsername(username);
    const mapSnap = await db.collection("usernames").doc(usernameLower).get();

    if (!mapSnap.exists) {
      return res.status(404).json({ error: "user not found" });
    }

    const { userId } = mapSnap.data();
    const userSnap = await db.collection("users").doc(userId).get();

    if (!userSnap.exists) {
      return res.status(404).json({ error: "user not found" });
    }

    res.json(serializeDoc(userSnap));
  } catch (err) {
    console.error("GET /users/resolve error:", err);
    res.status(500).json({ error: "failed to resolve user" });
  }
});

app.post("/players", async (req, res) => {
  try {
    const user = await ensureUserRecord({
      username: req.body.name,
      email: req.body.email ?? null,
    });
    res.status(201).json(user);
  } catch (err) {
    console.error("POST /players error:", err);
    res.status(400).json({ error: err.message });
  }
});

app.get("/users/:userId/sessions", async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const snap = await db
      .collection("sessions")
      .where("userId", "==", userId)
      .orderBy("startedAt", "desc")
      .limit(20)
      .get();

    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /users/:userId/sessions error:", err);
    res.status(500).json({ error: "failed to fetch user sessions" });
  }
});

app.get("/sessions", async (req, res) => {
  try {
    const userId = safeString(req.query.userId, null);

    let q = db.collection("sessions").orderBy("startedAt", "desc").limit(20);
    if (userId) {
      q = db
        .collection("sessions")
        .where("userId", "==", userId)
        .orderBy("startedAt", "desc")
        .limit(20);
    }

    const snap = await q.get();
    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /sessions error:", err);
    res.status(500).json({ error: "failed to fetch sessions" });
  }
});

app.post("/sessions", async (req, res) => {
  try {
    const userId = safeString(req.body.userId);
    const username = safeString(req.body.username, null);
    const email = safeString(req.body.email, null);

    let user = null;

    if (userId) {
      const userSnap = await db.collection("users").doc(userId).get();
      if (!userSnap.exists) {
        return res.status(404).json({ error: "user not found" });
      }
      user = serializeDoc(userSnap);
    } else if (username) {
      user = await ensureUserRecord({ username, email });
    } else {
      return res.status(400).json({ error: "userId or username is required" });
    }

    const trackName = safeString(req.body.trackName, null);
    const trackId = parseInteger(req.body.trackId, null);
    const sessionType = parseInteger(req.body.sessionType, null);

    const sessionRef = db.collection("sessions").doc();
    await sessionRef.set({
      userId: user.id,
      username: user.username,
      email: user.email ?? null,
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
        lastLapTimeMs: null,
      },
    });

    const saved = await sessionRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /sessions error:", err);
    res.status(500).json({ error: "failed to create session" });
  }
});

app.patch("/sessions/:id", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const updateBody = {};
    if (req.body.trackId !== undefined)
      updateBody.trackId = parseInteger(req.body.trackId, null);
    if (req.body.trackName !== undefined)
      updateBody.trackName = safeString(req.body.trackName, null);
    if (req.body.sessionType !== undefined)
      updateBody.sessionType = parseInteger(req.body.sessionType, null);
    if (req.body.playerName !== undefined)
      updateBody.playerName = safeString(req.body.playerName, null);

    await sessionRef.set(updateBody, { merge: true });

    const updated = await sessionRef.get();
    res.json(serializeDoc(updated));
  } catch (err) {
    console.error("PATCH /sessions/:id error:", err);
    res.status(500).json({ error: "failed to update session" });
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
    const sector1Ms = parseInteger(req.body.sector1Ms, null);
    const sector2Ms = parseInteger(req.body.sector2Ms, null);
    const sector3Ms = parseInteger(req.body.sector3Ms, null);
    const valid = parseBoolean(req.body.valid, true);
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

    const effectiveSector3Ms =
      sector3Ms ??
      (lapTimeMs != null && sector1Ms != null && sector2Ms != null
        ? Math.max(0, lapTimeMs - sector1Ms - sector2Ms)
        : null);

    const lapData = {
      lapNumber,
      lapTimeMs,
      sector1Ms,
      sector2Ms,
      sector3Ms: effectiveSector3Ms,
      valid,
      trackName,
      trackId,
      recordedAt: FieldValue.serverTimestamp(),
    };

    await lapRef.set(lapData);

    const currentSummary = sessionSnap.data()?.processedSummary || {};
    const lapIsValid = valid !== false;

    const nextBestLap =
      lapIsValid &&
      lapTimeMs != null &&
      (currentSummary.bestLapTimeMs == null || lapTimeMs < currentSummary.bestLapTimeMs)
        ? lapTimeMs
        : currentSummary.bestLapTimeMs ?? null;

    const nextFastestLap =
      lapIsValid &&
      lapTimeMs != null &&
      (!currentSummary.fastestLap || lapTimeMs < currentSummary.fastestLap.rawMs)
        ? {
            lapNumber,
            rawMs: lapTimeMs,
            formattedTime: formatLapTime(lapTimeMs),
          }
        : currentSummary.fastestLap ?? null;

    const nextSummary = {
      ...currentSummary,
      totalLaps: Math.max(parseInteger(currentSummary.totalLaps, 0) || 0, lapNumber),
      lastLapTimeMs: lapTimeMs,
      bestLapTimeMs: nextBestLap,
      fastestLap: nextFastestLap,
    };

    await sessionRef.set(
      {
        processedSummary: nextSummary,
      },
      { merge: true }
    );

    res.status(201).json({ success: true, lapNumber });
  } catch (err) {
    console.error("POST /sessions/:id/laps error:", err);
    res.status(500).json({ error: "failed to save lap" });
  }
});

app.post("/sessions/:id/corners", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const cornerRef = sessionRef.collection("corners").doc();

    await cornerRef.set({
      cornerIndex: parseInteger(req.body.cornerIndex, null),
      trackId: parseInteger(req.body.trackId, null),
      trackName: safeString(req.body.trackName, null),

      startedAt: safeString(req.body.startedAt, null),
      endedAt: safeString(req.body.endedAt, null),
      durationMs: parseInteger(req.body.durationMs, null),

      startLapNumber: parseInteger(req.body.startLapNumber, null),
      endLapNumber: parseInteger(req.body.endLapNumber, null),

      startLapDistanceM: parseNumber(req.body.startLapDistanceM, null),
      endLapDistanceM: parseNumber(req.body.endLapDistanceM, null),

      startTotalDistanceM: parseNumber(req.body.startTotalDistanceM, null),
      endTotalDistanceM: parseNumber(req.body.endTotalDistanceM, null),

      startSpeedKph: parseNumber(req.body.startSpeedKph, null),
      endSpeedKph: parseNumber(req.body.endSpeedKph, null),

      maxAbsSteering: parseNumber(req.body.maxAbsSteering, null),
      endReason: safeString(req.body.endReason, null),

      createdAt: FieldValue.serverTimestamp(),
    });

    const saved = await cornerRef.get();
    res.status(201).json(serializeDoc(saved));
  } catch (err) {
    console.error("POST /sessions/:id/corners error:", err);
    res.status(500).json({ error: "failed to save corner" });
  }
});

app.post("/telemetry/latest", async (req, res) => {
  try {
    const sessionId = safeString(req.body.sessionId);
    const latestTelemetry = req.body.latestTelemetry;

    if (!sessionId || !latestTelemetry) {
      return res
        .status(400)
        .json({ error: "sessionId and latestTelemetry required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    await sessionRef.set(
      {
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp(),
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
    }

    await chunkRef.set({
      samples,
      count: samples.length,
      receivedAt: FieldValue.serverTimestamp(),
    });

    const currentSummary = sessionSnap.data()?.processedSummary || {};

    const mergedSummary = {
      totalSamples: (parseInteger(currentSummary.totalSamples, 0) || 0) + samples.length,
      totalLaps: currentSummary.totalLaps || 0,
      topSpeedKph: Math.max(parseNumber(currentSummary.topSpeedKph, 0) || 0, maxSpeed),
      longestBrakingZoneMeters: Math.max(
        parseNumber(currentSummary.longestBrakingZoneMeters, 0) || 0,
        longestBrakingZoneMeters
      ),
      fastestLap: currentSummary.fastestLap ?? null,
      currentLapNumber: currentLapNumber ?? currentSummary.currentLapNumber ?? null,
      bestLapTimeMs: currentSummary.bestLapTimeMs ?? null,
      lastLapTimeMs: currentSummary.lastLapTimeMs ?? null,
    };

    await sessionRef.set(
      {
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp(),
        processedSummary: mergedSummary,
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
      "users",
      "usernames",
      "sessions",
      "sessions/{sessionId}/telemetryChunks",
      "sessions/{sessionId}/laps",
      "sessions/{sessionId}/corners",
    ],
    strategy:
      "Users are the identity root. Sessions reference userId and store telemetry + summary.",
  });
});

async function start() {
  app.listen(port, () => console.log("API RUNNING on port", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});