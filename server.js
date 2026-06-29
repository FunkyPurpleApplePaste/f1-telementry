// backend_with_map.js
// Drop-in replacement for your Express/Firebase backend.
// Keeps your existing user/session/lap/corner/telemetry routes.
// Adds map-ready storage:
//   - latestTelemetry.worldX/worldY/worldZ
//   - sessions/{sessionId}.latestMapPosition
//   - sessions/{sessionId}.mapSummary
//   - sessions/{sessionId}/telemetryChunks/{chunkId}.mapBounds
//   - trackMaps/{trackKey}
//   - trackMaps/{trackKey}/centerlineChunks/{chunkId}

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
app.use(express.json({ limit: "10mb" }));
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

function parseBoolean(value, fallback = null) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;

  const s = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(s)) return true;
  if (["false", "0", "no", "n", "off"].includes(s)) return false;
  return fallback;
}

function stripUndefinedDeep(value) {
  if (Array.isArray(value)) {
    return value.map(stripUndefinedDeep);
  }

  if (value && typeof value === "object") {
    const out = {};
    for (const [key, child] of Object.entries(value)) {
      if (child !== undefined) {
        out[key] = stripUndefinedDeep(child);
      }
    }
    return out;
  }

  return value;
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
    "updatedAt",
    "calibratedAt",
    "finalizedAt",
    "createdFromSessionAt",
  ]) {
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
  return `${minutes}:${seconds.toString().padStart(2, "0")}.${fraction
    .toString()
    .padStart(3, "0")}`;
}

function trackKeyFrom(trackId, trackName) {
  const id = parseInteger(trackId, null);
  if (id !== null) return `track_${id}`;

  const name = normalizeUsername(trackName || "unknown_track");
  return `track_${name || "unknown_track"}`;
}

function finiteNumberOrNull(value) {
  const n = parseNumber(value, null);
  return n === null ? null : n;
}

function extractMapPoint(sample) {
  if (!sample || typeof sample !== "object") return null;

  const mapPosition = sample.mapPosition || {};
  const worldX = finiteNumberOrNull(sample.worldX ?? mapPosition.worldX);
  const worldY = finiteNumberOrNull(sample.worldY ?? mapPosition.worldY);
  const worldZ = finiteNumberOrNull(sample.worldZ ?? mapPosition.worldZ);

  if (worldX === null || worldZ === null) return null;

  return {
    timestamp: safeString(sample.timestamp, null),
    sampleIndex: parseInteger(sample.sampleIndex, null),

    lapNumber: parseInteger(sample.lapNumber, null),
    lapDistance: finiteNumberOrNull(sample.lapDistance),
    totalDistance: finiteNumberOrNull(sample.totalDistance),

    worldX,
    worldY,
    worldZ,

    yaw: finiteNumberOrNull(sample.yaw),
    pitch: finiteNumberOrNull(sample.pitch),
    roll: finiteNumberOrNull(sample.roll),

    speedKph: finiteNumberOrNull(sample.speedKph),
    throttle: finiteNumberOrNull(sample.throttle),
    brake: finiteNumberOrNull(sample.brake),
    steering: finiteNumberOrNull(sample.steering),

    trackId: parseInteger(sample.trackId, null),
    trackName: safeString(sample.trackName, null),
  };
}

function makeBoundsFromPoint(point) {
  if (!point) return null;

  const bounds = {
    minX: point.worldX,
    maxX: point.worldX,
    minZ: point.worldZ,
    maxZ: point.worldZ,
  };

  if (point.worldY !== null) {
    bounds.minY = point.worldY;
    bounds.maxY = point.worldY;
  }

  return withBoundDimensions(bounds);
}

function withBoundDimensions(bounds) {
  if (!bounds) return null;

  const minX = finiteNumberOrNull(bounds.minX);
  const maxX = finiteNumberOrNull(bounds.maxX);
  const minZ = finiteNumberOrNull(bounds.minZ);
  const maxZ = finiteNumberOrNull(bounds.maxZ);

  if (minX === null || maxX === null || minZ === null || maxZ === null) return null;

  const out = {
    minX,
    maxX,
    minZ,
    maxZ,
    widthMeters: maxX - minX,
    heightMeters: maxZ - minZ,
  };

  const minY = finiteNumberOrNull(bounds.minY);
  const maxY = finiteNumberOrNull(bounds.maxY);
  if (minY !== null && maxY !== null) {
    out.minY = minY;
    out.maxY = maxY;
    out.elevationRangeMeters = maxY - minY;
  }

  return out;
}

function mergeBounds(existing, incoming) {
  const a = withBoundDimensions(existing);
  const b = withBoundDimensions(incoming);

  if (!a) return b;
  if (!b) return a;

  const merged = {
    minX: Math.min(a.minX, b.minX),
    maxX: Math.max(a.maxX, b.maxX),
    minZ: Math.min(a.minZ, b.minZ),
    maxZ: Math.max(a.maxZ, b.maxZ),
  };

  const aMinY = finiteNumberOrNull(a.minY);
  const aMaxY = finiteNumberOrNull(a.maxY);
  const bMinY = finiteNumberOrNull(b.minY);
  const bMaxY = finiteNumberOrNull(b.maxY);
  if (aMinY !== null || bMinY !== null) {
    merged.minY = Math.min(aMinY ?? bMinY, bMinY ?? aMinY);
    merged.maxY = Math.max(aMaxY ?? bMaxY, bMaxY ?? aMaxY);
  }

  return withBoundDimensions(merged);
}

function boundsFromPoints(points) {
  let bounds = null;
  for (const point of points) {
    bounds = mergeBounds(bounds, makeBoundsFromPoint(point));
  }
  return bounds;
}

function buildLatestMapPosition(sample) {
  const point = extractMapPoint(sample);
  if (!point) return null;

  return {
    worldX: point.worldX,
    worldY: point.worldY,
    worldZ: point.worldZ,
    yaw: point.yaw,
    pitch: point.pitch,
    roll: point.roll,
    lapDistance: point.lapDistance,
    totalDistance: point.totalDistance,
    lapNumber: point.lapNumber,
    speedKph: point.speedKph,
    throttle: point.throttle,
    brake: point.brake,
    steering: point.steering,
    timestamp: point.timestamp,
    sampleIndex: point.sampleIndex,
  };
}

function buildMapStats(samples) {
  const points = [];
  let maxSpeed = 0;
  let longestBrakingZoneMeters = 0;
  let currentLapNumber = null;

  for (const raw of samples) {
    const speedKph = finiteNumberOrNull(raw?.speedKph);
    if (speedKph !== null && speedKph > maxSpeed) {
      maxSpeed = speedKph;
    }

    const brakingDistance = finiteNumberOrNull(raw?.brakingDistance);
    if (brakingDistance !== null && brakingDistance > longestBrakingZoneMeters) {
      longestBrakingZoneMeters = brakingDistance;
    }

    const lapNumber = parseInteger(raw?.lapNumber, null);
    if (lapNumber !== null) {
      currentLapNumber = lapNumber;
    }

    const point = extractMapPoint(raw);
    if (point) points.push(point);
  }

  return {
    totalSamples: samples.length,
    maxSpeed,
    longestBrakingZoneMeters,
    currentLapNumber,
    mapPointCount: points.length,
    bounds: boundsFromPoints(points),
    latestMapPosition: points.length ? buildLatestMapPosition(points[points.length - 1]) : null,
    mapPreviewPoints: downsamplePoints(points, 150).map(pointToSmallMapPoint),
  };
}

function pointToSmallMapPoint(point) {
  return {
    t: point.timestamp,
    i: point.sampleIndex,
    lap: point.lapNumber,
    d: point.lapDistance,
    x: point.worldX,
    y: point.worldY,
    z: point.worldZ,
    v: point.speedKph,
    br: point.brake,
    th: point.throttle,
    st: point.steering,
  };
}

function downsamplePoints(points, maxPoints = 500) {
  if (!Array.isArray(points) || points.length <= maxPoints) return points;
  const out = [];
  const step = (points.length - 1) / (maxPoints - 1);

  for (let i = 0; i < maxPoints; i += 1) {
    out.push(points[Math.round(i * step)]);
  }

  return out;
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

function mergeMapSummary(existing = {}, stats = {}, latestMapPosition = null, sessionData = {}) {
  const existingCount = parseInteger(existing.sampleCount, 0) || 0;
  const incomingCount = parseInteger(stats.mapPointCount, 0) || 0;

  const bounds = mergeBounds(existing.worldBounds, stats.bounds);
  const trackId = parseInteger(sessionData.trackId ?? existing.trackId, null);
  const trackName = safeString(sessionData.trackName ?? existing.trackName, null);

  return stripUndefinedDeep({
    hasWorldPosition: incomingCount > 0 || existing.hasWorldPosition === true,
    sampleCount: existingCount + incomingCount,
    worldBounds: bounds,
    latestMapPosition: latestMapPosition ?? existing.latestMapPosition ?? null,
    trackId,
    trackName,
    trackKey: trackKeyFrom(trackId, trackName),
  });
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

  await sessionRef.set(stripUndefinedDeep(updateBody), { merge: true });
}

async function mergeGlobalTrackMap(sessionData, stats, latestMapPosition) {
  if (!stats || stats.mapPointCount <= 0 || !stats.bounds) return null;

  const trackId = parseInteger(sessionData.trackId, null);
  const trackName = safeString(sessionData.trackName, null);
  const trackKey = trackKeyFrom(trackId, trackName);
  const ref = db.collection("trackMaps").doc(trackKey);
  const snap = await ref.get();
  const existing = snap.exists ? snap.data() || {} : {};

  const mergedBounds = mergeBounds(existing.worldBounds, stats.bounds);
  const existingCount = parseInteger(existing.sampleCount, 0) || 0;

  const updateBody = {
    trackKey,
    trackId,
    trackName,
    worldBounds: mergedBounds,
    sampleCount: existingCount + stats.mapPointCount,
    latestMapPosition: latestMapPosition ?? existing.latestMapPosition ?? null,
    updatedAt: FieldValue.serverTimestamp(),
  };

  if (!snap.exists) {
    updateBody.createdAt = FieldValue.serverTimestamp();
  }

  await ref.set(stripUndefinedDeep(updateBody), { merge: true });
  return trackKey;
}

function sortMapPoints(points) {
  return [...points].sort((a, b) => {
    const aLap = parseInteger(a.lapNumber, 0) || 0;
    const bLap = parseInteger(b.lapNumber, 0) || 0;
    if (aLap !== bLap) return aLap - bLap;

    const aDist = finiteNumberOrNull(a.lapDistance);
    const bDist = finiteNumberOrNull(b.lapDistance);
    if (aDist !== null && bDist !== null && aDist !== bDist) return aDist - bDist;

    const ai = parseInteger(a.sampleIndex, 0) || 0;
    const bi = parseInteger(b.sampleIndex, 0) || 0;
    return ai - bi;
  });
}

async function collectSessionMapPoints(sessionRef) {
  const chunksSnap = await sessionRef.collection("telemetryChunks").get();
  const points = [];

  for (const doc of chunksSnap.docs) {
    const data = doc.data() || {};
    const samples = Array.isArray(data.samples) ? data.samples : [];

    for (const sample of samples) {
      const point = extractMapPoint(sample);
      if (point) points.push(point);
    }
  }

  return sortMapPoints(points);
}

async function saveCenterlineChunks(trackKey, points, version, chunkSize = 400) {
  const ref = db.collection("trackMaps").doc(trackKey);
  const batchLimit = 400;
  let batch = db.batch();
  let writes = 0;
  let chunkIndex = 0;

  for (let i = 0; i < points.length; i += chunkSize) {
    const chunkPoints = points.slice(i, i + chunkSize).map(pointToSmallMapPoint);
    const chunkRef = ref.collection("centerlineChunks").doc(`v${version}_chunk_${String(chunkIndex).padStart(3, "0")}`);

    batch.set(chunkRef, {
      version,
      chunkIndex,
      count: chunkPoints.length,
      points: chunkPoints,
      createdAt: FieldValue.serverTimestamp(),
    });

    writes += 1;
    chunkIndex += 1;

    if (writes >= batchLimit) {
      await batch.commit();
      batch = db.batch();
      writes = 0;
    }
  }

  if (writes > 0) {
    await batch.commit();
  }

  return chunkIndex;
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
    const trackKey = trackKeyFrom(trackId, trackName);

    const sessionRef = db.collection("sessions").doc();
    await sessionRef.set(stripUndefinedDeep({
      userId: user.id,
      username: user.username,
      email: user.email ?? null,
      trackName,
      trackId,
      trackKey,
      sessionType,
      startedAt: FieldValue.serverTimestamp(),
      endedAt: null,
      latestTelemetry: null,
      latestTelemetryAt: null,
      latestMapPosition: null,
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
      mapSummary: {
        hasWorldPosition: false,
        sampleCount: 0,
        worldBounds: null,
        latestMapPosition: null,
        trackId,
        trackName,
        trackKey,
      },
    }));

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

    const current = sessionSnap.data() || {};
    const updateBody = {};
    if (req.body.trackId !== undefined)
      updateBody.trackId = parseInteger(req.body.trackId, null);
    if (req.body.trackName !== undefined)
      updateBody.trackName = safeString(req.body.trackName, null);
    if (req.body.sessionType !== undefined)
      updateBody.sessionType = parseInteger(req.body.sessionType, null);
    if (req.body.playerName !== undefined)
      updateBody.playerName = safeString(req.body.playerName, null);

    const nextTrackId = updateBody.trackId ?? current.trackId ?? null;
    const nextTrackName = updateBody.trackName ?? current.trackName ?? null;
    updateBody.trackKey = trackKeyFrom(nextTrackId, nextTrackName);

    await sessionRef.set(stripUndefinedDeep(updateBody), { merge: true });

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
    const valid = parseBoolean(req.body.valid, false);
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

    const lapData = stripUndefinedDeep({
      lapNumber,
      lapTimeMs,
      sector1Ms,
      sector2Ms,
      sector3Ms: effectiveSector3Ms,
      valid,
      trackName,
      trackId,
      recordedAt: FieldValue.serverTimestamp(),
    });

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

    await cornerRef.set(stripUndefinedDeep({
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
      minSpeedKph: parseNumber(req.body.minSpeedKph, null),
      maxBrake: parseNumber(req.body.maxBrake, null),
      maxThrottle: parseNumber(req.body.maxThrottle, null),

      maxAbsSteering: parseNumber(req.body.maxAbsSteering, null),
      sampleCount: parseInteger(req.body.sampleCount, null),

      startWorldX: parseNumber(req.body.startWorldX, null),
      startWorldY: parseNumber(req.body.startWorldY, null),
      startWorldZ: parseNumber(req.body.startWorldZ, null),
      endWorldX: parseNumber(req.body.endWorldX, null),
      endWorldY: parseNumber(req.body.endWorldY, null),
      endWorldZ: parseNumber(req.body.endWorldZ, null),

      apexLapDistanceM: parseNumber(req.body.apexLapDistanceM, null),
      apexWorldX: parseNumber(req.body.apexWorldX, null),
      apexWorldY: parseNumber(req.body.apexWorldY, null),
      apexWorldZ: parseNumber(req.body.apexWorldZ, null),

      endReason: safeString(req.body.endReason, null),

      createdAt: FieldValue.serverTimestamp(),
    }));

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

    const latestMapPosition = buildLatestMapPosition(latestTelemetry);

    const updateBody = {
      latestTelemetry,
      latestTelemetryAt: FieldValue.serverTimestamp(),
    };

    if (latestMapPosition) {
      updateBody.latestMapPosition = latestMapPosition;
      updateBody["mapSummary.latestMapPosition"] = latestMapPosition;
      updateBody["mapSummary.hasWorldPosition"] = true;
    }

    await sessionRef.set(stripUndefinedDeep(updateBody), { merge: true });

    res.json({ success: true, hasMapPosition: !!latestMapPosition });
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

    const sessionData = sessionSnap.data() || {};
    const stats = buildMapStats(samples);
    const latestTelemetry = samples[samples.length - 1];
    const latestMapPosition = stats.latestMapPosition ?? buildLatestMapPosition(latestTelemetry);

    const chunkRef = sessionRef.collection("telemetryChunks").doc();

    await chunkRef.set(stripUndefinedDeep({
      samples,
      count: samples.length,
      mapPointCount: stats.mapPointCount,
      mapBounds: stats.bounds,
      mapPreviewPoints: stats.mapPreviewPoints,
      receivedAt: FieldValue.serverTimestamp(),
    }));

    const currentSummary = sessionData.processedSummary || {};
    const mergedSummary = mergeProcessedSummary(currentSummary, {
      totalSamples: samples.length,
      topSpeedKph: stats.maxSpeed,
      longestBrakingZoneMeters: stats.longestBrakingZoneMeters,
      currentLapNumber: stats.currentLapNumber,
    });

    const mergedMapSummary = mergeMapSummary(
      sessionData.mapSummary || {},
      stats,
      latestMapPosition,
      sessionData
    );

    await sessionRef.set(
      stripUndefinedDeep({
        latestTelemetry,
        latestTelemetryAt: FieldValue.serverTimestamp(),
        latestMapPosition: latestMapPosition ?? sessionData.latestMapPosition ?? null,
        processedSummary: mergedSummary,
        mapSummary: mergedMapSummary,
      }),
      { merge: true }
    );

    const trackKey = await mergeGlobalTrackMap(sessionData, stats, latestMapPosition);

    res.status(201).json({
      success: true,
      count: samples.length,
      mapPointCount: stats.mapPointCount,
      trackKey,
      mapBounds: stats.bounds,
    });
  } catch (err) {
    console.error("POST /telemetry/batch error:", err);
    res.status(500).json({ error: "failed to save telemetry batch" });
  }
});

app.post("/sessions/:id/track-map/finalize", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const maxPoints = parseInteger(req.body.maxPoints, 800) || 800;

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const sessionData = sessionSnap.data() || {};
    const points = await collectSessionMapPoints(sessionRef);

    if (points.length === 0) {
      return res.status(400).json({
        error: "no world position samples found for this session",
        hint: "Make sure the listener is receiving Motion packet 0 and sending worldX/worldZ.",
      });
    }

    const bounds = boundsFromPoints(points);
    const centerline = downsamplePoints(points, maxPoints);
    const trackId = parseInteger(sessionData.trackId, null);
    const trackName = safeString(sessionData.trackName, null);
    const trackKey = trackKeyFrom(trackId, trackName);
    const version = Date.now();

    const trackMap = stripUndefinedDeep({
      trackKey,
      trackId,
      trackName,
      sourceSessionId: sessionId,
      worldBounds: bounds,
      sourceSampleCount: points.length,
      centerlinePointCount: centerline.length,
      centerlineVersion: version,
      finalizedAt: FieldValue.serverTimestamp(),
    });

    await sessionRef.set(
      {
        trackMap,
        mapSummary: mergeMapSummary(sessionData.mapSummary || {}, {
          mapPointCount: points.length,
          bounds,
        }, buildLatestMapPosition(points[points.length - 1]), sessionData),
      },
      { merge: true }
    );

    const trackRef = db.collection("trackMaps").doc(trackKey);
    const existingTrackSnap = await trackRef.get();
    const existingTrack = existingTrackSnap.exists ? existingTrackSnap.data() || {} : {};

    await trackRef.set(
      stripUndefinedDeep({
        trackKey,
        trackId,
        trackName,
        worldBounds: mergeBounds(existingTrack.worldBounds, bounds),
        sourceSessionId: sessionId,
        sourceSampleCount: points.length,
        centerlinePointCount: centerline.length,
        centerlineVersion: version,
        updatedAt: FieldValue.serverTimestamp(),
        finalizedAt: FieldValue.serverTimestamp(),
        createdAt: existingTrackSnap.exists ? existingTrack.createdAt : FieldValue.serverTimestamp(),
      }),
      { merge: true }
    );

    const chunkCount = await saveCenterlineChunks(trackKey, centerline, version);

    res.status(201).json({
      success: true,
      sessionId,
      trackKey,
      worldBounds: bounds,
      sourceSampleCount: points.length,
      centerlinePointCount: centerline.length,
      centerlineChunkCount: chunkCount,
      centerlineVersion: version,
    });
  } catch (err) {
    console.error("POST /sessions/:id/track-map/finalize error:", err);
    res.status(500).json({ error: "failed to finalize track map" });
  }
});

app.get("/sessions/:id/track-map", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const data = sessionSnap.data() || {};
    res.json({
      sessionId,
      trackKey: data.trackKey ?? data.mapSummary?.trackKey ?? null,
      latestMapPosition: data.latestMapPosition ?? null,
      mapSummary: data.mapSummary ?? null,
      trackMap: data.trackMap ?? null,
    });
  } catch (err) {
    console.error("GET /sessions/:id/track-map error:", err);
    res.status(500).json({ error: "failed to fetch session track map" });
  }
});

app.get("/track-maps/:trackKey", async (req, res) => {
  try {
    const trackKey = safeString(req.params.trackKey);
    if (!trackKey) return res.status(400).json({ error: "trackKey is required" });

    const trackRef = db.collection("trackMaps").doc(trackKey);
    const snap = await trackRef.get();

    if (!snap.exists) {
      return res.status(404).json({ error: "track map not found" });
    }

    const trackMap = serializeDoc(snap);
    const includeCenterline = parseBoolean(req.query.includeCenterline, false);

    if (!includeCenterline) {
      return res.json(trackMap);
    }

    const version = parseInteger(req.query.version, trackMap.centerlineVersion ?? null);

    // Fetch and sort in app code to avoid requiring a Firestore composite index.
    const chunksSnap = await trackRef.collection("centerlineChunks").get();
    const chunkDocs = chunksSnap.docs
      .map((doc) => doc.data() || {})
      .filter((data) => version === null || parseInteger(data.version, null) === version)
      .sort((a, b) => (parseInteger(a.chunkIndex, 0) || 0) - (parseInteger(b.chunkIndex, 0) || 0));

    const centerline = [];
    for (const data of chunkDocs) {
      if (Array.isArray(data.points)) centerline.push(...data.points);
    }

    res.json({
      ...trackMap,
      centerline,
    });
  } catch (err) {
    console.error("GET /track-maps/:trackKey error:", err);
    res.status(500).json({ error: "failed to fetch track map" });
  }
});

app.patch("/track-maps/:trackKey/calibration", async (req, res) => {
  try {
    const trackKey = safeString(req.params.trackKey);
    if (!trackKey) return res.status(400).json({ error: "trackKey is required" });

    const imageWidth = parseNumber(req.body.imageWidth, null);
    const imageHeight = parseNumber(req.body.imageHeight, null);
    const imageUrl = safeString(req.body.imageUrl, null);

    const anchorPoints = Array.isArray(req.body.anchorPoints)
      ? req.body.anchorPoints
          .map((p) => ({
            label: safeString(p.label, null),
            worldX: parseNumber(p.worldX, null),
            worldZ: parseNumber(p.worldZ, null),
            imageX: parseNumber(p.imageX, null),
            imageY: parseNumber(p.imageY, null),
          }))
          .filter(
            (p) =>
              p.worldX !== null &&
              p.worldZ !== null &&
              p.imageX !== null &&
              p.imageY !== null
          )
      : [];

    const calibration = stripUndefinedDeep({
      imageUrl,
      imageWidth,
      imageHeight,
      anchorPoints,
      note:
        "Use anchorPoints to transform worldX/worldZ into imageX/imageY on the frontend.",
      calibratedAt: FieldValue.serverTimestamp(),
    });

    const ref = db.collection("trackMaps").doc(trackKey);
    await ref.set(
      {
        imageCalibration: calibration,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const saved = await ref.get();
    res.json(serializeDoc(saved));
  } catch (err) {
    console.error("PATCH /track-maps/:trackKey/calibration error:", err);
    res.status(500).json({ error: "failed to save calibration" });
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
      "trackMaps",
      "trackMaps/{trackKey}/centerlineChunks",
    ],
    importantSessionFields: {
      latestTelemetry: "Latest raw telemetry sample, including worldX/worldY/worldZ.",
      latestMapPosition: "Small map-ready current car position.",
      mapSummary: "Session-level world bounds and map sample count.",
      trackMap: "Finalized map generated from a calibration/session recording.",
    },
    importantTrackMapFields: {
      worldBounds: "min/max world X/Z and calculated width/height in game metres.",
      imageCalibration:
        "Optional image size and anchor points used to align world coordinates to a track image.",
      centerlineChunks:
        "Downsampled points for drawing the track path without overloading one Firestore document.",
    },
    strategy:
      "Sessions store driving data. trackMaps store reusable circuit calibration data for map overlays.",
  });
});

async function start() {
  app.listen(port, () => console.log("API RUNNING on port", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});