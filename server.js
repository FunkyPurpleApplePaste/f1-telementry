import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import dotenv from "dotenv";
import bcrypt from "bcryptjs";
import crypto from "crypto";

import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore, FieldValue, FieldPath } from "firebase-admin/firestore";

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;
const PASSWORD_MIN_LENGTH = 8;
const BCRYPT_SALT_ROUNDS = Number(process.env.BCRYPT_SALT_ROUNDS || 12);
const AUTH_SESSION_DAYS = Number(process.env.AUTH_SESSION_DAYS || 7);
const LIVE_STREAM_HEARTBEAT_MS = Number(process.env.LIVE_STREAM_HEARTBEAT_MS || 15000);
const LATEST_FIRESTORE_WRITE_INTERVAL_MS = Number(
  process.env.LATEST_FIRESTORE_WRITE_INTERVAL_MS || 100
);

let credential;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  credential = cert(serviceAccount);
} else {
  credential = applicationDefault();
}

initializeApp({ credential });
const firebaseAuth = getAuth();
const db = getFirestore();

const liveTelemetryCache = new Map();
const liveStreamClients = new Map();
const latestFirestoreWriteAtBySession = new Map();

function getLiveClientSet(sessionId) {
  if (!liveStreamClients.has(sessionId)) {
    liveStreamClients.set(sessionId, new Set());
  }

  return liveStreamClients.get(sessionId);
}

function writeLiveStreamEvent(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function buildLiveTelemetryPayload(
  sessionId,
  latestTelemetry,
  latestMapPosition,
  sessionData = {}
) {
  return stripUndefinedDeep({
    sessionId,
    latestTelemetry: latestTelemetry ?? null,
    latestTelemetryFreshness: telemetryFreshness(latestTelemetry),
    latestMapPosition: latestMapPosition ?? null,
    trackKey:
      sessionData.trackKey ??
      sessionData.mapSummary?.trackKey ??
      trackKeyFrom(sessionData.trackId, sessionData.trackName),
    trackId: parseInteger(sessionData.trackId, null),
    trackName: safeString(sessionData.trackName, null),
    serverSentAt: new Date().toISOString(),
  });
}

function broadcastLiveTelemetry(sessionId, payload) {
  if (!sessionId || !payload) return;

  liveTelemetryCache.set(sessionId, payload);
  const clients = liveStreamClients.get(sessionId);
  if (!clients || clients.size === 0) return;

  for (const res of [...clients]) {
    if (res.destroyed || res.writableEnded) {
      clients.delete(res);
      continue;
    }

    try {
      writeLiveStreamEvent(res, "telemetry", payload);
    } catch {
      clients.delete(res);
    }
  }
}

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || "2mb" }));
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

function parseClientEndDate(value) {
  const raw = safeString(value, null);
  if (!raw) return null;

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;

  const now = Date.now();
  const maxFutureMs = 5 * 60 * 1000;
  if (parsed.getTime() > now + maxFutureMs) {
    return new Date(now);
  }

  return parsed;
}

function buildEndMetadata(req, fallbackSource = "server_received") {
  const endedAt =
    parseClientEndDate(req.body?.endedAt) ||
    parseClientEndDate(req.body?.endDetectedAt) ||
    parseClientEndDate(req.body?.endedAtIso);

  return stripUndefinedDeep({
    endedAt,
    endedAtSource:
      safeString(req.body?.endedAtSource, null) ||
      safeString(req.body?.endSource, null) ||
      (endedAt ? "listener_detected" : fallbackSource),
    endReason:
      safeString(req.body?.endReason, null) ||
      safeString(req.body?.reason, null),
    endPacketType: safeString(req.body?.endPacketType, null),
    listenerClosedAt: parseClientEndDate(req.body?.listenerClosedAt),
  });
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
    "lastUsedAt",
    "suspendedAt",
    "updatedAt",
    "calibratedAt",
    "finalizedAt",
    "createdFromSessionAt",
    "expiresAt",
    "revokedAt",
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

// LEADERBOARD_FUNCTIONAL_PATCH
const LEADERBOARD_DEFAULT_LIMIT = Number(process.env.LEADERBOARD_DEFAULT_LIMIT || 50);
const LEADERBOARD_SESSION_SCAN_LIMIT = Number(process.env.LEADERBOARD_SESSION_SCAN_LIMIT || 250);
const LEADERBOARD_MIN_VALID_LAP_MS = Number(process.env.LEADERBOARD_MIN_VALID_LAP_MS || 10000);
const LEADERBOARD_MAX_VALID_LAP_MS = Number(process.env.LEADERBOARD_MAX_VALID_LAP_MS || 600000);

function leaderboardTimestampMs(value) {
  if (!value) return 0;
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (typeof value === "number") return value;
  return 0;
}

function isRealValidLeaderboardLap(lap) {
  const lapTimeMs = parseInteger(lap?.lapTimeMs, null);
  return (
    lap?.valid === true &&
    lapTimeMs !== null &&
    lapTimeMs >= LEADERBOARD_MIN_VALID_LAP_MS &&
    lapTimeMs <= LEADERBOARD_MAX_VALID_LAP_MS
  );
}

function leaderboardUserKey(sessionData) {
  return (
    safeString(sessionData.userId, null) ||
    normalizeUsername(sessionData.username) ||
    normalizeEmail(sessionData.email) ||
    null
  );
}

function buildLeaderboardEntry(sessionDoc, sessionData, lapDoc, lapData) {
  const lapTimeMs = parseInteger(lapData.lapTimeMs, null);
  const trackId = parseInteger(lapData.trackId ?? sessionData.trackId, null);
  const trackName = safeString(lapData.trackName ?? sessionData.trackName, null);
  const trackKey = safeString(sessionData.trackKey, null) || trackKeyFrom(trackId, trackName);

  return stripUndefinedDeep({
    userId: safeString(sessionData.userId, null),
    username: safeString(sessionData.username, "Unknown Driver"),
    email: safeString(sessionData.email, null),
    sessionId: sessionDoc.id,
    lapId: lapDoc.id,
    lapNumber: parseInteger(lapData.lapNumber, null),
    lapTimeMs,
    lapTime: formatLapTime(lapTimeMs),
    sector1Ms: parseInteger(lapData.sector1Ms, null),
    sector2Ms: parseInteger(lapData.sector2Ms, null),
    sector3Ms: parseInteger(lapData.sector3Ms, null),
    valid: lapData.valid === true,
    trackName,
    trackId,
    trackKey,
    sessionType: parseInteger(sessionData.sessionType, null),
    sessionStartedAt: sessionData.startedAt || null,
    recordedAt: lapData.recordedAt || null,
    sortStartedAtMs: leaderboardTimestampMs(sessionData.startedAt),
    sortRecordedAtMs: leaderboardTimestampMs(lapData.recordedAt),
  });
}

function rankLeaderboardEntries(entries) {
  const sorted = [...entries].sort((a, b) => {
    if (a.lapTimeMs !== b.lapTimeMs) return a.lapTimeMs - b.lapTimeMs;
    if (a.sortRecordedAtMs !== b.sortRecordedAtMs) return b.sortRecordedAtMs - a.sortRecordedAtMs;
    return String(a.username || "").localeCompare(String(b.username || ""));
  });

  const leaderTime = sorted[0]?.lapTimeMs ?? null;

  return sorted.map((entry, index) =>
    stripUndefinedDeep({
      rank: index + 1,
      ...entry,
      gapToLeaderMs: leaderTime !== null ? entry.lapTimeMs - leaderTime : null,
      sortStartedAtMs: undefined,
      sortRecordedAtMs: undefined,
    })
  );
}

function finiteNumberOrNull(value) {
  const n = parseNumber(value, null);
  return n === null ? null : n;
}

function telemetryTimestampMillis(value) {
  if (!value) return null;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value.seconds === "number") {
    const nanos = Number(value.nanoseconds || 0);
    return value.seconds * 1000 + Math.floor(nanos / 1000000);
  }

  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function telemetryFreshness(sample) {
  if (!sample || typeof sample !== "object") {
    return {
      sampleIndex: null,
      gameTimeMs: null,
      lapNumber: null,
      lapDistance: null,
      timestampMs: null,
    };
  }

  return {
    sampleIndex: parseInteger(sample.sampleIndex ?? sample.i, null),
    gameTimeMs: finiteNumberOrNull(
      sample.gameTimeMs ??
        sample.gameTime ??
        sample.sessionTimeMs ??
        sample.sessionTime
    ),
    lapNumber: parseInteger(sample.lapNumber ?? sample.lap, null),
    lapDistance: finiteNumberOrNull(sample.lapDistance ?? sample.d),
    timestampMs: telemetryTimestampMillis(sample.timestamp ?? sample.t),
  };
}

function compareTelemetryFreshness(candidate, current) {
  const next = telemetryFreshness(candidate);
  const prev = telemetryFreshness(current);

  for (const key of ["sampleIndex", "gameTimeMs", "timestampMs"]) {
    if (next[key] !== null && prev[key] !== null) {
      const diff = next[key] - prev[key];
      if (diff !== 0) return diff;
    }
  }

  if (
    next.lapNumber !== null &&
    prev.lapNumber !== null &&
    next.lapNumber !== prev.lapNumber
  ) {
    return next.lapNumber - prev.lapNumber;
  }

  if (
    next.lapDistance !== null &&
    prev.lapDistance !== null &&
    next.lapDistance !== prev.lapDistance
  ) {
    return next.lapDistance - prev.lapDistance;
  }

  const nextHasSignal = Object.values(next).some((value) => value !== null);
  const prevHasSignal = Object.values(prev).some((value) => value !== null);
  if (nextHasSignal && !prevHasSignal) return 1;
  if (!nextHasSignal && prevHasSignal) return -1;
  return 0;
}

function isNewerTelemetrySample(candidate, current) {
  if (!candidate) return false;
  if (!current) return true;
  return compareTelemetryFreshness(candidate, current) > 0;
}

const MAP_TELEPORT_JUMP_METERS = Number(process.env.MAP_TELEPORT_JUMP_METERS || 180);

function mapPointDistanceMeters(a, b) {
  if (!a || !b) return 0;
  const dx = Number(a.worldX) - Number(b.worldX);
  const dz = Number(a.worldZ) - Number(b.worldZ);
  if (!Number.isFinite(dx) || !Number.isFinite(dz)) return 0;
  return Math.sqrt(dx * dx + dz * dz);
}

function isUsableMapPoint(point) {
  if (!point) return false;
  if (point.pitStatus !== null && point.pitStatus > 0) return false;
  if (point.lapNumber === 1 && point.lapDistance !== null && point.lapDistance < -5) return false;
  return true;
}

function isTeleportMapJump(previous, current) {
  if (!previous || !current) return false;
  if (previous.lapNumber !== null && current.lapNumber !== null && previous.lapNumber !== current.lapNumber) {
    return false;
  }

  const jumpMeters = mapPointDistanceMeters(previous, current);
  if (jumpMeters >= MAP_TELEPORT_JUMP_METERS) return true;

  if (
    previous.lapDistance !== null &&
    current.lapDistance !== null &&
    current.lapDistance + 25 < previous.lapDistance
  ) {
    return true;
  }

  return false;
}


function extractMapPoint(sample) {
  if (!sample || typeof sample !== "object") return null;

  const mapPosition = sample.mapPosition || {};
  const worldX = finiteNumberOrNull(sample.worldX ?? mapPosition.worldX ?? sample.x);
  const worldY = finiteNumberOrNull(sample.worldY ?? mapPosition.worldY ?? sample.y);
  const worldZ = finiteNumberOrNull(sample.worldZ ?? mapPosition.worldZ ?? sample.z);

  if (worldX === null || worldZ === null) return null;

  return {
    timestamp: safeString(sample.timestamp ?? sample.t, null),
    sampleIndex: parseInteger(sample.sampleIndex ?? sample.i, null),

    lapNumber: parseInteger(sample.lapNumber ?? sample.lap, null),
    lapDistance: finiteNumberOrNull(sample.lapDistance ?? sample.d),
    totalDistance: finiteNumberOrNull(sample.totalDistance ?? sample.td),

    worldX,
    worldY,
    worldZ,

    yaw: finiteNumberOrNull(sample.yaw),
    pitch: finiteNumberOrNull(sample.pitch),
    roll: finiteNumberOrNull(sample.roll),

    speedKph: finiteNumberOrNull(sample.speedKph ?? sample.v),
    throttle: finiteNumberOrNull(sample.throttle ?? sample.th),
    brake: finiteNumberOrNull(sample.brake ?? sample.br),
    steering: finiteNumberOrNull(sample.steering ?? sample.st),
    pitStatus: parseInteger(sample.pitStatus ?? sample.pit, null),

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
    if (isUsableMapPoint(point)) points.push(point);
  }

  return {
    totalSamples: samples.length,
    maxSpeed,
    longestBrakingZoneMeters,
    currentLapNumber,
    mapPointCount: points.rawPointCount ?? points.length,
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
    pit: point.pitStatus,
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

function toPublicUser(user) {
  const { passwordHash, ...safe } = user || {};
  const isAdmin = safe.role === "admin" || safe.isAdmin === true;

  return {
    ...safe,
    role: isAdmin ? "admin" : safe.role || "user",
    isAdmin,
    isSuspended: safe.isSuspended === true,
  };
}

async function createFirebaseCustomToken(user) {
  const publicUser = toPublicUser(user);

  return firebaseAuth.createCustomToken(publicUser.id, {
    role: publicUser.role || "user",
    isAdmin: publicUser.isAdmin === true,
    username: publicUser.username || "",
  });
}

function isSuspendedUser(user) {
  return user?.isSuspended === true || Boolean(user?.suspendedAt);
}

function hasEndedAt(value) {
  if (!value) return false;
  if (typeof value.toDate === "function") return true;
  if (typeof value.toMillis === "function") return true;
  if (typeof value.seconds === "number") return true;
  if (typeof value === "string") return value.trim() !== "";
  return true;
}

function isClosedSession(session) {
  return Boolean(session) && hasEndedAt(session.endedAt);
}

function authTokenHash(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

function getAuthSessionExpiry() {
  return new Date(Date.now() + AUTH_SESSION_DAYS * 24 * 60 * 60 * 1000);
}

function asDate(value) {
  if (!value) return null;
  if (typeof value.toDate === "function") return value.toDate();

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

async function createAuthSession(userId) {
  const token = crypto.randomBytes(32).toString("hex");
  const tokenHash = authTokenHash(token);

  await db.collection("authSessions").doc(tokenHash).set({
    userId,
    createdAt: FieldValue.serverTimestamp(),
    expiresAt: getAuthSessionExpiry(),
  });

  return token;
}

function getBearerToken(req) {
  const authorization = req.get("authorization") || "";
  return authorization.toLowerCase().startsWith("bearer ")
    ? authorization.slice(7).trim()
    : null;
}

async function findUserByIdentifier(identifier) {
  const value = safeString(identifier, null);
  if (!value) return null;

  const emailLower = normalizeEmail(value);
  const usernameLower = normalizeUsername(value);
  const lookups = [];

  if (emailLower && value.includes("@")) {
    lookups.push({ collection: "emails", key: emailLower });
  }

  if (usernameLower) {
    lookups.push({ collection: "usernames", key: usernameLower });
  }

  for (const lookup of lookups) {
    const mapSnap = await db.collection(lookup.collection).doc(lookup.key).get();
    if (!mapSnap.exists) continue;

    const { userId } = mapSnap.data() || {};
    if (!userId) continue;

    const userRef = db.collection("users").doc(userId);
    const userSnap = await userRef.get();
    if (userSnap.exists) {
      return {
        id: userSnap.id,
        ref: userRef,
        data: userSnap.data() || {},
      };
    }
  }

  if (emailLower) {
    const snap = await db
      .collection("users")
      .where("emailLower", "==", emailLower)
      .limit(1)
      .get();

    if (!snap.empty) {
      const userSnap = snap.docs[0];
      return {
        id: userSnap.id,
        ref: userSnap.ref,
        data: userSnap.data() || {},
      };
    }
  }

  return null;
}

async function authenticate(req, res, next) {
  try {
    const token = getBearerToken(req);

    if (!token) {
      return res.status(401).json({ error: "login required" });
    }

    const sessionRef = db.collection("authSessions").doc(authTokenHash(token));
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(401).json({ error: "invalid login session" });
    }

    const session = sessionSnap.data() || {};
    const expiresAt = asDate(session.expiresAt);

    if (!session.userId || !expiresAt || expiresAt <= new Date()) {
      await sessionRef.delete().catch(() => {});
      return res.status(401).json({ error: "login session expired" });
    }

    const userRef = db.collection("users").doc(session.userId);
    const userSnap = await userRef.get();

    if (!userSnap.exists) {
      return res.status(401).json({ error: "user no longer exists" });
    }

    req.authSessionRef = sessionRef;
    req.userRef = userRef;
    req.user = toPublicUser({ id: userSnap.id, ...(userSnap.data() || {}) });

    if (isSuspendedUser(req.user)) {
      return res.status(403).json({ error: "account is suspended" });
    }

    next();
  } catch (err) {
    console.error("Auth middleware error:", err);
    res.status(500).json({ error: "failed to verify login session" });
  }
}

async function optionalAuthenticate(req, res, next) {
  try {
    const token = getBearerToken(req);

    if (!token) {
      return next();
    }

    const sessionRef = db.collection("authSessions").doc(authTokenHash(token));
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return next();
    }

    const session = sessionSnap.data() || {};
    const expiresAt = asDate(session.expiresAt);

    if (!session.userId || !expiresAt || expiresAt <= new Date()) {
      await sessionRef.delete().catch(() => {});
      return next();
    }

    const userRef = db.collection("users").doc(session.userId);
    const userSnap = await userRef.get();

    if (!userSnap.exists) {
      return next();
    }

    req.authSessionRef = sessionRef;
    req.userRef = userRef;
    req.user = toPublicUser({ id: userSnap.id, ...(userSnap.data() || {}) });

    if (isSuspendedUser(req.user)) {
      return res.status(403).json({ error: "account is suspended" });
    }

    return next();
  } catch (err) {
    console.error("Optional auth middleware error:", err);
    return next();
  }
}

function getListenerToken(req) {
  const headerToken = safeString(req.get("x-listener-token"), null);
  if (headerToken) return headerToken;

  const authorization = req.get("authorization") || "";
  if (authorization.toLowerCase().startsWith("listener ")) {
    return authorization.slice(9).trim();
  }

  return safeString(req.body?.listenerToken, null);
}

async function createListenerToken(userId, label = "F1 listener") {
  const token = `f1lt_${crypto.randomBytes(32).toString("hex")}`;
  const tokenHash = authTokenHash(token);
  const tokenId = crypto.randomUUID();
  const tokenPreview = `${token.slice(0, 12)}...${token.slice(-6)}`;

  await db.collection("listenerTokens").doc(tokenHash).set({
    tokenId,
    userId,
    label: safeString(label, "F1 listener"),
    tokenPreview,
    createdAt: FieldValue.serverTimestamp(),
    lastUsedAt: null,
    revokedAt: null,
  });

  return {
    token,
    tokenId,
    tokenPreview,
  };
}

async function resolveListenerUser(req) {
  const token = getListenerToken(req);
  if (!token) return null;

  const tokenRef = db.collection("listenerTokens").doc(authTokenHash(token));
  const tokenSnap = await tokenRef.get();

  if (!tokenSnap.exists) {
    const err = new Error("invalid listener token");
    err.status = 401;
    throw err;
  }

  const tokenData = tokenSnap.data() || {};

  if (tokenData.revokedAt) {
    const err = new Error("listener token has been revoked");
    err.status = 401;
    throw err;
  }

  const userSnap = await db.collection("users").doc(tokenData.userId).get();
  if (!userSnap.exists) {
    const err = new Error("listener token user was not found");
    err.status = 401;
    throw err;
  }

  const user = toPublicUser(serializeDoc(userSnap));
  if (isSuspendedUser(user)) {
    const err = new Error("account is suspended");
    err.status = 403;
    throw err;
  }

  await tokenRef.set(
    {
      lastUsedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  await userSnap.ref.set(
    {
      lastSeenAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  return user;
}

function serializeListenerToken(docSnap) {
  const data = serializeDoc(docSnap);
  return {
    id: data.tokenId || docSnap.id,
    tokenId: data.tokenId || docSnap.id,
    label: data.label || "F1 listener",
    tokenPreview: data.tokenPreview || null,
    createdAt: data.createdAt || null,
    lastUsedAt: data.lastUsedAt || null,
    revokedAt: data.revokedAt || null,
  };
}

function requireAdmin(req, res, next) {
  if (!req.user?.isAdmin) {
    return res.status(403).json({ error: "admin access required" });
  }

  next();
}

function canReadUserSessions(req, userId) {
  return Boolean(req.user?.isAdmin || (userId && req.user?.id === userId));
}

function canReadSession(req, sessionData) {
  return Boolean(req.user?.isAdmin || (sessionData?.userId && req.user?.id === sessionData.userId));
}

function requireReadableSession(req, res, sessionData) {
  if (!canReadSession(req, sessionData)) {
    res.status(403).json({ error: "session access denied" });
    return false;
  }

  return true;
}

async function deleteQueryInBatches(queryRef, batchSize = 400) {
  while (true) {
    const snap = await queryRef.limit(batchSize).get();
    if (snap.empty) return;

    const batch = db.batch();
    for (const doc of snap.docs) {
      batch.delete(doc.ref);
    }
    await batch.commit();

    if (snap.size < batchSize) return;
  }
}

async function deleteSessionWithChildren(sessionRef) {
  for (const subcollection of ["telemetryChunks", "laps", "corners", "reports"]) {
    await deleteQueryInBatches(sessionRef.collection(subcollection));
  }

  await sessionRef.delete();
}

async function listUserSessions(userId, max = 50) {
  const snap = await db
    .collection("sessions")
    .where("userId", "==", userId)
    .orderBy("startedAt", "desc")
    .limit(max)
    .get();

  return snap.docs.map(serializeDoc);
}

async function ensureUserRecord({ username, email = null }) {
  const cleanUsername = safeString(username);
  if (!cleanUsername) throw new Error("username is required");

  const usernameLower = normalizeUsername(cleanUsername);
  const emailValue = safeString(email, null);
  const emailLower = normalizeEmail(emailValue);

  const usernameRef = db.collection("usernames").doc(usernameLower);
  const emailRef = emailLower ? db.collection("emails").doc(emailLower) : null;

  return await db.runTransaction(async (tx) => {
    const mapSnap = await tx.get(usernameRef);
    const emailSnap = emailRef ? await tx.get(emailRef) : null;

    if (mapSnap.exists) {
      const { userId } = mapSnap.data();
      const userRef = db.collection("users").doc(userId);
      const userSnap = await tx.get(userRef);

      if (!userSnap.exists) {
        throw new Error("username mapping is broken");
      }

      if (emailSnap?.exists && emailSnap.data()?.userId !== userId) {
        throw new Error("email is already in use");
      }

      const current = userSnap.data() || {};
      const updateBody = {
        username: cleanUsername,
        usernameLower,
        role: current.role || (current.isAdmin ? "admin" : "user"),
        lastSeenAt: FieldValue.serverTimestamp(),
      };

      if (emailValue) {
        updateBody.email = emailValue;
        updateBody.emailLower = emailLower;
      }

      tx.set(userRef, updateBody, { merge: true });

      if (emailRef) {
        const emailMap = {
          userId,
          emailLower,
          updatedAt: FieldValue.serverTimestamp(),
        };

        if (!emailSnap?.exists) {
          emailMap.createdAt = FieldValue.serverTimestamp();
        }

        tx.set(emailRef, emailMap, { merge: true });
      }

      return toPublicUser({
        id: userId,
        ...current,
        ...updateBody,
      });
    }

    if (emailSnap?.exists) {
      throw new Error("email is already in use");
    }

    const userRef = db.collection("users").doc();
    const userData = {
      username: cleanUsername,
      usernameLower,
      email: emailValue,
      emailLower,
      role: "user",
      isAdmin: false,
      createdAt: FieldValue.serverTimestamp(),
      lastSeenAt: FieldValue.serverTimestamp(),
    };

    tx.set(userRef, userData);
    tx.set(usernameRef, {
      userId: userRef.id,
      usernameLower,
      createdAt: FieldValue.serverTimestamp(),
    });

    if (emailRef) {
      tx.set(emailRef, {
        userId: userRef.id,
        emailLower,
        createdAt: FieldValue.serverTimestamp(),
      });
    }

    return toPublicUser({
      id: userRef.id,
      ...userData,
    });
  });
}

async function createAccount({ username, email, password }) {
  const cleanUsername = safeString(username);
  const emailValue = safeString(email, null);
  const emailLower = normalizeEmail(emailValue);

  if (!cleanUsername) throw new Error("name is required");
  if (!emailLower) throw new Error("valid email is required");
  if (!password || String(password).length < PASSWORD_MIN_LENGTH) {
    throw new Error(`password must be at least ${PASSWORD_MIN_LENGTH} characters`);
  }

  const usernameLower = normalizeUsername(cleanUsername);
  const existingEmailUser = await findUserByIdentifier(emailLower);

  if (existingEmailUser && existingEmailUser.data.usernameLower !== usernameLower) {
    throw new Error("email is already in use");
  }

  const usernameRef = db.collection("usernames").doc(usernameLower);
  const emailRef = db.collection("emails").doc(emailLower);
  const passwordHash = await bcrypt.hash(String(password), BCRYPT_SALT_ROUNDS);

  return await db.runTransaction(async (tx) => {
    const usernameSnap = await tx.get(usernameRef);
    const emailSnap = await tx.get(emailRef);

    if (usernameSnap.exists) {
      const { userId } = usernameSnap.data() || {};
      const userRef = db.collection("users").doc(userId);
      const userSnap = await tx.get(userRef);

      if (!userSnap.exists) {
        throw new Error("username mapping is broken");
      }

      const current = userSnap.data() || {};

      if (current.passwordHash) {
        throw new Error("name is already in use");
      }

      if (emailSnap.exists && emailSnap.data()?.userId !== userId) {
        throw new Error("email is already in use");
      }

      const updateBody = {
        username: cleanUsername,
        usernameLower,
        email: emailValue,
        emailLower,
        passwordHash,
        role: current.role || (current.isAdmin ? "admin" : "user"),
        isAdmin: current.isAdmin === true || current.role === "admin",
        lastSeenAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      };

      tx.set(userRef, updateBody, { merge: true });
      tx.set(emailRef, {
        userId,
        emailLower,
        updatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });

      return toPublicUser({
        id: userId,
        ...current,
        ...updateBody,
      });
    }

    if (emailSnap.exists) {
      throw new Error("email is already in use");
    }

    const userRef = db.collection("users").doc();
    const userData = {
      username: cleanUsername,
      usernameLower,
      email: emailValue,
      emailLower,
      passwordHash,
      role: "user",
      isAdmin: false,
      createdAt: FieldValue.serverTimestamp(),
      lastSeenAt: FieldValue.serverTimestamp(),
    };

    tx.set(userRef, userData);
    tx.set(usernameRef, {
      userId: userRef.id,
      usernameLower,
      createdAt: FieldValue.serverTimestamp(),
    });
    tx.set(emailRef, {
      userId: userRef.id,
      emailLower,
      createdAt: FieldValue.serverTimestamp(),
    });

    return toPublicUser({
      id: userRef.id,
      ...userData,
    });
  });
}

async function loginAccount({ identifier, password }) {
  const found = await findUserByIdentifier(identifier);
  if (!found?.data?.passwordHash) return null;

  if (isSuspendedUser(found.data)) {
    const err = new Error("account is suspended");
    err.status = 403;
    throw err;
  }

  const matches = await bcrypt.compare(String(password ?? ""), found.data.passwordHash);
  if (!matches) return null;

  await found.ref.set(
    {
      lastSeenAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  const token = await createAuthSession(found.id);
  const freshSnap = await found.ref.get();

  return {
    token,
    user: toPublicUser(serializeDoc(freshSnap)),
  };
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
    processedSummary: mergedSummary,
  };

  if (
    patch.latestTelemetry !== undefined &&
    isNewerTelemetrySample(patch.latestTelemetry, current.latestTelemetry)
  ) {
    updateBody.latestTelemetry = patch.latestTelemetry;
    updateBody.latestTelemetryFreshness = telemetryFreshness(patch.latestTelemetry);
    updateBody.latestTelemetryAt = FieldValue.serverTimestamp();
  }

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

async function collectSessionMapPoints(sessionRef, options = {}) {
  let points = [];
  let rawPointCount = 0;
  let lastDoc = null;
  const maxPoints = Math.max(100, parseInteger(options.maxPoints, MAX_SESSION_MAP_POINTS) || MAX_SESSION_MAP_POINTS);

  while (true) {
    let queryRef = sessionRef
      .collection("telemetryChunks")
      .orderBy(FieldPath.documentId())
      .limit(MAP_POINT_CHUNK_PAGE_SIZE);

    if (lastDoc) {
      queryRef = queryRef.startAfter(lastDoc);
    }

    const chunksSnap = await queryRef.get();
    if (chunksSnap.empty) break;

    for (const doc of chunksSnap.docs) {
      const data = doc.data() || {};
      const previewPoints = Array.isArray(data.mapPreviewPoints) ? data.mapPreviewPoints : [];
      const samples = previewPoints.length ? previewPoints : (Array.isArray(data.samples) ? data.samples : []);

      for (const sample of samples) {
        const point = extractMapPoint(sample);
        if (!isUsableMapPoint(point)) continue;

        rawPointCount += 1;
        points.push(point);

        if (points.length > Math.ceil(maxPoints * 1.25)) {
          points = downsamplePoints(points, maxPoints);
        }
      }
    }

    lastDoc = chunksSnap.docs[chunksSnap.docs.length - 1];
    if (chunksSnap.size < MAP_POINT_CHUNK_PAGE_SIZE) break;
  }

  const sorted = sortMapPoints(downsamplePoints(points, maxPoints));
  Object.defineProperty(sorted, "rawPointCount", {
    value: rawPointCount,
    enumerable: false,
  });

  return sorted;
}

function buildLapTrailsFromMapPoints(points, maxPointsPerLap = 1200) {
  const grouped = new Map();
  const segmentByLap = new Map();
  const lastPointByLap = new Map();

  for (const point of sortMapPoints(points)) {
    if (point.lapNumber == null) continue;

    const baseKey = `lap-${point.lapNumber}`;
    const previousPoint = lastPointByLap.get(point.lapNumber) || null;
    const shouldStartNewSegment = isTeleportMapJump(previousPoint, point);

    if (!segmentByLap.has(point.lapNumber)) {
      segmentByLap.set(point.lapNumber, 1);
    } else if (shouldStartNewSegment) {
      segmentByLap.set(point.lapNumber, segmentByLap.get(point.lapNumber) + 1);
    }

    const segmentIndex = segmentByLap.get(point.lapNumber);
    const key = segmentIndex === 1 ? baseKey : `${baseKey}-segment-${segmentIndex}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        key,
        lapNumber: point.lapNumber,
        label:
          segmentIndex === 1
            ? `Lap ${point.lapNumber} Trail`
            : `Lap ${point.lapNumber} Trail Part ${segmentIndex}`,
        pointCount: 0,
        originalPointCount: 0,
        points: [],
        startedAt: point.timestamp || null,
        endedAt: null,
      });
    }

    const trail = grouped.get(key);
    trail.points.push({
      timestamp: point.timestamp,
      sampleIndex: point.sampleIndex,
      lapNumber: point.lapNumber,
      lapDistance: point.lapDistance,
      totalDistance: point.totalDistance,
      worldX: point.worldX,
      worldY: point.worldY,
      worldZ: point.worldZ,
      speedKph: point.speedKph,
      throttle: point.throttle,
      brake: point.brake,
      steering: point.steering,
    });
    trail.originalPointCount += 1;
    trail.endedAt = point.timestamp || trail.endedAt;
    lastPointByLap.set(point.lapNumber, point);
  }

  return [...grouped.values()]
    .filter((trail) => trail.points.length >= 2)
    .map((trail) => {
      const downsampled = downsamplePoints(trail.points, maxPointsPerLap);
      return {
        ...trail,
        points: downsampled,
        pointCount: downsampled.length,
      };
    })
    .sort((a, b) => a.lapNumber - b.lapNumber);
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

app.post("/auth/signup", async (req, res) => {
  try {
    if (req.body.password !== req.body.confirmPassword && req.body.confirmPassword !== undefined) {
      return res.status(400).json({ error: "passwords do not match" });
    }

    const createdUser = await createAccount({
      username: req.body.username ?? req.body.name,
      email: req.body.email,
      password: req.body.password,
    });

    const userSnap = await db.collection("users").doc(createdUser.id).get();
    const user = toPublicUser(serializeDoc(userSnap));
    const token = await createAuthSession(user.id);
    const firebaseToken = await createFirebaseCustomToken(user);

    res.status(201).json({ user, token, firebaseToken });
  } catch (err) {
    console.error("POST /auth/signup error:", err);
    res.status(400).json({ error: err.message });
  }
});

app.post("/auth/login", async (req, res) => {
  try {
    const result = await loginAccount({
      identifier: req.body.identifier ?? req.body.username ?? req.body.email,
      password: req.body.password,
    });

    if (!result) {
      return res.status(401).json({ error: "invalid name/email or password" });
    }

    const firebaseToken = await createFirebaseCustomToken(result.user);

    res.json({ ...result, firebaseToken });
  } catch (err) {
    console.error("POST /auth/login error:", err);
    res.status(err.status || 500).json({ error: err.message || "failed to log in" });
  }
});

app.get("/auth/me", authenticate, async (req, res) => {
  res.json({ user: req.user });
});

app.post("/auth/logout", authenticate, async (req, res) => {
  await req.authSessionRef.delete();
  res.json({ ok: true });
});

app.get("/listener-tokens", authenticate, async (req, res) => {
  try {
    const snap = await db
      .collection("listenerTokens")
      .where("userId", "==", req.user.id)
      .get();

    const tokens = snap.docs
      .map(serializeListenerToken)
      .filter((token) => !token.revokedAt)
      .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));

    res.json({ tokens });
  } catch (err) {
    console.error("GET /listener-tokens error:", err);
    res.status(500).json({ error: "failed to load listener tokens" });
  }
});

app.post("/listener-tokens", authenticate, async (req, res) => {
  try {
    const tokenData = await createListenerToken(req.user.id, req.body.label);
    res.status(201).json(tokenData);
  } catch (err) {
    console.error("POST /listener-tokens error:", err);
    res.status(500).json({ error: "failed to create listener token" });
  }
});

app.delete("/listener-tokens/:tokenId", authenticate, async (req, res) => {
  try {
    const tokenId = safeString(req.params.tokenId);
    if (!tokenId) return res.status(400).json({ error: "tokenId is required" });

    const snap = await db
      .collection("listenerTokens")
      .where("userId", "==", req.user.id)
      .where("tokenId", "==", tokenId)
      .limit(1)
      .get();

    if (snap.empty) {
      return res.status(404).json({ error: "listener token not found" });
    }

    await snap.docs[0].ref.set(
      {
        revokedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("DELETE /listener-tokens/:tokenId error:", err);
    res.status(500).json({ error: "failed to revoke listener token" });
  }
});

app.get("/admin/users", authenticate, requireAdmin, async (req, res) => {
  try {
    const snap = await db.collection("users").orderBy("createdAt", "desc").limit(200).get();
    const users = snap.docs.map((doc) => toPublicUser(serializeDoc(doc)));
    res.json({ users });
  } catch (err) {
    console.error("GET /admin/users error:", err);
    res.status(500).json({ error: "failed to load users" });
  }
});

app.get("/admin/users/:userId/sessions", authenticate, requireAdmin, async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const userSnap = await db.collection("users").doc(userId).get();
    if (!userSnap.exists) return res.status(404).json({ error: "user not found" });

    const sessions = await listUserSessions(userId, 100);
    res.json({ sessions });
  } catch (err) {
    console.error("GET /admin/users/:userId/sessions error:", err);
    res.status(500).json({ error: "failed to load user sessions" });
  }
});

app.patch("/admin/users/:userId", authenticate, requireAdmin, async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const nextUsername = safeString(req.body.username, null);
    const nextEmail = safeString(req.body.email, null);
    const nextRole = safeString(req.body.role, null);
    const nextSuspended = parseBoolean(req.body.isSuspended, null);
    const suspendedReason = safeString(req.body.suspendedReason, null);

    if (nextRole && !["user", "admin"].includes(nextRole)) {
      return res.status(400).json({ error: "role must be user or admin" });
    }

    if (req.user.id === userId && nextRole === "user") {
      return res.status(400).json({ error: "you cannot remove your own admin role" });
    }

    if (req.user.id === userId && nextSuspended === true) {
      return res.status(400).json({ error: "you cannot suspend your own account" });
    }

    const userRef = db.collection("users").doc(userId);

    await db.runTransaction(async (tx) => {
      const userSnap = await tx.get(userRef);
      if (!userSnap.exists) throw new Error("user not found");

      const current = userSnap.data() || {};
      const updateBody = {
        updatedAt: FieldValue.serverTimestamp(),
      };

      const currentUsernameLower = current.usernameLower || normalizeUsername(current.username);
      const currentEmailLower = current.emailLower || normalizeEmail(current.email);

      if (nextUsername) {
        const usernameLower = normalizeUsername(nextUsername);
        if (!usernameLower) throw new Error("username is required");

        if (usernameLower !== currentUsernameLower) {
          const usernameRef = db.collection("usernames").doc(usernameLower);
          const usernameSnap = await tx.get(usernameRef);

          if (usernameSnap.exists && usernameSnap.data()?.userId !== userId) {
            throw new Error("username is already in use");
          }

          tx.set(usernameRef, {
            userId,
            usernameLower,
            updatedAt: FieldValue.serverTimestamp(),
          }, { merge: true });

          if (currentUsernameLower) {
            tx.delete(db.collection("usernames").doc(currentUsernameLower));
          }
        }

        updateBody.username = nextUsername;
        updateBody.usernameLower = usernameLower;
      }

      if (nextEmail) {
        const emailLower = normalizeEmail(nextEmail);
        if (!emailLower) throw new Error("valid email is required");

        if (emailLower !== currentEmailLower) {
          const emailRef = db.collection("emails").doc(emailLower);
          const emailSnap = await tx.get(emailRef);

          if (emailSnap.exists && emailSnap.data()?.userId !== userId) {
            throw new Error("email is already in use");
          }

          tx.set(emailRef, {
            userId,
            emailLower,
            updatedAt: FieldValue.serverTimestamp(),
          }, { merge: true });

          if (currentEmailLower) {
            tx.delete(db.collection("emails").doc(currentEmailLower));
          }
        }

        updateBody.email = nextEmail;
        updateBody.emailLower = emailLower;
      }

      if (nextRole) {
        updateBody.role = nextRole;
        updateBody.isAdmin = nextRole === "admin";
      }

      if (nextSuspended !== null) {
        updateBody.isSuspended = nextSuspended;
        updateBody.suspendedReason = nextSuspended ? suspendedReason : null;
        updateBody.suspendedAt = nextSuspended ? FieldValue.serverTimestamp() : null;
      }

      tx.set(userRef, stripUndefinedDeep(updateBody), { merge: true });
    });

    const updated = await userRef.get();
    res.json({ user: toPublicUser(serializeDoc(updated)) });
  } catch (err) {
    console.error("PATCH /admin/users/:userId error:", err);
    const status = err.message === "user not found" ? 404 : 400;
    res.status(status).json({ error: err.message || "failed to update user" });
  }
});

app.delete("/admin/users/:userId/sessions", authenticate, requireAdmin, async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const sessions = await listUserSessions(userId, 500);
    for (const session of sessions) {
      await deleteSessionWithChildren(db.collection("sessions").doc(session.id));
    }

    res.json({ deletedCount: sessions.length });
  } catch (err) {
    console.error("DELETE /admin/users/:userId/sessions error:", err);
    res.status(500).json({ error: "failed to delete user sessions" });
  }
});

app.delete("/admin/users/:userId/sessions/:sessionId", authenticate, requireAdmin, async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    const sessionId = safeString(req.params.sessionId);
    if (!userId || !sessionId) {
      return res.status(400).json({ error: "userId and sessionId are required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const session = sessionSnap.data() || {};
    if (session.userId !== userId) {
      return res.status(400).json({ error: "session does not belong to this user" });
    }

    await deleteSessionWithChildren(sessionRef);
    res.json({ ok: true });
  } catch (err) {
    console.error("DELETE /admin/users/:userId/sessions/:sessionId error:", err);
    res.status(500).json({ error: "failed to delete session" });
  }
});

app.post("/listener/resolve", async (req, res) => {
  try {
    const user = await resolveListenerUser(req);
    if (!user) {
      return res.status(401).json({ error: "listener token is required" });
    }

    res.json({ user });
  } catch (err) {
    console.error("POST /listener/resolve error:", err);
    res.status(err.status || 500).json({ error: err.message || "failed to resolve listener token" });
  }
});

app.post("/users/ensure", async (req, res) => {
  try {
    const listenerUser = await resolveListenerUser(req);
    if (listenerUser) {
      return res.status(200).json(listenerUser);
    }

    const user = await ensureUserRecord({
      username: req.body.username,
      email: req.body.email ?? null,
    });

    if (isSuspendedUser(user)) {
      return res.status(403).json({ error: "account is suspended" });
    }

    res.status(201).json(toPublicUser(user));
  } catch (err) {
    console.error("POST /users/ensure error:", err);
    res.status(err.status || 400).json({ error: err.message });
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

    res.json(toPublicUser(serializeDoc(userSnap)));
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

    if (isSuspendedUser(user)) {
      return res.status(403).json({ error: "account is suspended" });
    }

    res.status(201).json(toPublicUser(user));
  } catch (err) {
    console.error("POST /players error:", err);
    res.status(400).json({ error: err.message });
  }
});

app.get("/users/:userId/sessions", authenticate, async (req, res) => {
  try {
    const userId = safeString(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId is required" });
    if (!canReadUserSessions(req, userId)) {
      return res.status(403).json({ error: "session access denied" });
    }

    const snap = await db
      .collection("sessions")
      .where("userId", "==", userId)
      .orderBy("startedAt", "desc")
      .limit(50)
      .get();

    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /users/:userId/sessions error:", err);
    res.status(500).json({ error: "failed to fetch user sessions" });
  }
});

app.get("/sessions", authenticate, async (req, res) => {
  try {
    const requestedUserId = safeString(req.query.userId, null);
    const wantsAll =
      parseBoolean(req.query.all, false) === true ||
      safeString(req.query.scope, null) === "all";
    const effectiveUserId =
      req.user?.isAdmin && wantsAll ? null : requestedUserId || req.user?.id;

    if (requestedUserId && !canReadUserSessions(req, requestedUserId)) {
      return res.status(403).json({ error: "session access denied" });
    }

    if (!req.user?.isAdmin && !effectiveUserId) {
      return res.status(403).json({ error: "session access denied" });
    }

    let q = db.collection("sessions").orderBy("startedAt", "desc").limit(50);
    if (effectiveUserId) {
      q = db
        .collection("sessions")
        .where("userId", "==", effectiveUserId)
        .orderBy("startedAt", "desc")
        .limit(50);
    }

    const snap = await q.get();
    res.json(snap.docs.map(serializeDoc));
  } catch (err) {
    console.error("GET /sessions error:", err);
    res.status(500).json({ error: "failed to fetch sessions" });
  }
});

app.get("/leaderboard", async (req, res) => {
  try {
    const limitValue = parseInteger(req.query.limit, LEADERBOARD_DEFAULT_LIMIT) || LEADERBOARD_DEFAULT_LIMIT;
    const limitRows = Math.max(1, Math.min(limitValue, 100));
    const scanLimitValue = parseInteger(req.query.scanLimit, LEADERBOARD_SESSION_SCAN_LIMIT) || LEADERBOARD_SESSION_SCAN_LIMIT;
    const sessionScanLimit = Math.max(limitRows, Math.min(scanLimitValue, 500));
    const requestedTrackKey = safeString(req.query.trackKey, null);
    const trackIdFilter = parseInteger(req.query.trackId, null);
    const trackNameFilter = safeString(req.query.trackName, null);
    const normalizedTrackNameFilter = trackNameFilter ? normalizeUsername(trackNameFilter) : null;

    const sessionsSnap = await db
      .collection("sessions")
      .orderBy("startedAt", "desc")
      .limit(sessionScanLimit)
      .get();

    const bestByTrackAndUser = new Map();
    const trackStats = new Map();
    let scannedSessions = 0;
    let scannedLaps = 0;
    let validLaps = 0;

    function ensureTrack(entry) {
      const key = entry.trackKey || trackKeyFrom(entry.trackId, entry.trackName);
      if (!trackStats.has(key)) {
        trackStats.set(key, {
          trackKey: key,
          trackName: entry.trackName || key,
          trackId: entry.trackId ?? null,
          validLaps: 0,
          userKeys: new Set(),
          bestLapTimeMs: null,
          latestActivityMs: 0,
        });
      }
      return trackStats.get(key);
    }

    for (const sessionDoc of sessionsSnap.docs) {
      scannedSessions += 1;
      const sessionData = serializeDoc(sessionDoc);
      const userKey = leaderboardUserKey(sessionData);
      if (!userKey) continue;

      const lapsSnap = await sessionDoc.ref.collection("laps").get();

      for (const lapDoc of lapsSnap.docs) {
        scannedLaps += 1;
        const lapData = serializeDoc(lapDoc);
        if (!isRealValidLeaderboardLap(lapData)) continue;

        const entry = buildLeaderboardEntry(sessionDoc, sessionData, lapDoc, lapData);
        if (!entry.trackKey) continue;

        const stats = ensureTrack(entry);
        stats.validLaps += 1;
        stats.userKeys.add(userKey);
        stats.latestActivityMs = Math.max(
          stats.latestActivityMs,
          entry.sortRecordedAtMs || entry.sortStartedAtMs || 0
        );
        if (stats.bestLapTimeMs === null || entry.lapTimeMs < stats.bestLapTimeMs) {
          stats.bestLapTimeMs = entry.lapTimeMs;
        }

        validLaps += 1;
        if (!bestByTrackAndUser.has(entry.trackKey)) {
          bestByTrackAndUser.set(entry.trackKey, new Map());
        }

        const bestByUser = bestByTrackAndUser.get(entry.trackKey);
        const existing = bestByUser.get(userKey);

        if (
          !existing ||
          entry.lapTimeMs < existing.lapTimeMs ||
          (entry.lapTimeMs === existing.lapTimeMs && entry.sortRecordedAtMs > existing.sortRecordedAtMs)
        ) {
          bestByUser.set(userKey, entry);
        }
      }
    }

    const tracks = [...trackStats.values()]
      .map((track) => ({
        trackKey: track.trackKey,
        trackName: track.trackName,
        trackId: track.trackId,
        validLaps: track.validLaps,
        userCount: track.userKeys.size,
        bestLapTimeMs: track.bestLapTimeMs,
        bestLapTime: formatLapTime(track.bestLapTimeMs),
        latestActivityMs: track.latestActivityMs,
      }))
      .sort((a, b) => {
        if (b.latestActivityMs !== a.latestActivityMs) return b.latestActivityMs - a.latestActivityMs;
        return String(a.trackName || "").localeCompare(String(b.trackName || ""));
      });

    let activeTrack = null;
    if (requestedTrackKey) {
      activeTrack = tracks.find((track) => track.trackKey === requestedTrackKey) || null;
    } else if (trackIdFilter !== null) {
      activeTrack = tracks.find((track) => track.trackId === trackIdFilter) || null;
    } else if (normalizedTrackNameFilter) {
      activeTrack =
        tracks.find((track) => normalizeUsername(track.trackName || "") === normalizedTrackNameFilter) ||
        null;
    }

    if (!activeTrack) {
      activeTrack = tracks[0] || null;
    }

    const activeTrackKey = activeTrack?.trackKey || null;
    const activeBestByUser = activeTrackKey
      ? bestByTrackAndUser.get(activeTrackKey) || new Map()
      : new Map();
    const rows = rankLeaderboardEntries([...activeBestByUser.values()]).slice(0, limitRows);

    res.json({
      rows,
      tracks,
      activeTrackKey,
      activeTrack,
      filters: {
        trackKey: activeTrackKey,
        trackId: activeTrack?.trackId ?? trackIdFilter,
        trackName: activeTrack?.trackName ?? trackNameFilter,
      },
      meta: {
        leaderboardType: "best_valid_actual_lap_per_user_per_track",
        trackScoped: true,
        rules: [
          "Leaderboards are separated by track.",
          "Only laps with valid === true are eligible.",
          "Lap time must be a real lapTimeMs, not theoretical best.",
          "Each user appears once per track with their fastest eligible lap.",
        ],
        scannedSessions,
        scannedLaps,
        validLaps,
        trackCount: tracks.length,
        userCount: activeBestByUser.size,
      },
    });
  } catch (err) {
    console.error("GET /leaderboard error:", err);
    res.status(500).json({ error: "failed to fetch leaderboard" });
  }
});

app.post("/sessions", async (req, res) => {
  try {
    const listenerUser = await resolveListenerUser(req);
    const userId = safeString(req.body.userId);
    const username = safeString(req.body.username, null);
    const email = safeString(req.body.email, null);

    let user = null;

    if (listenerUser) {
      user = listenerUser;
    } else if (userId) {
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

    if (isSuspendedUser(user)) {
      return res.status(403).json({ error: "account is suspended" });
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
    res.status(err.status || 500).json({ error: err.message || "failed to create session" });
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

// POST_SESSION_AI_REPORT_START
const POST_SESSION_REPORT_SCHEMA = "f1-coach-evidence-report-v4";
const LOW_MEMORY_REPORT_PATCH_APPLIED = true;
const REPORT_TELEMETRY_CHUNK_PAGE_SIZE = Number(process.env.POST_SESSION_CHUNK_PAGE_SIZE || 2);
const MAX_REPORT_SAMPLES_PER_LAP = Number(process.env.POST_SESSION_MAX_SAMPLES_PER_LAP || 120);
const MAX_REPORT_TOTAL_SAMPLES = Number(process.env.POST_SESSION_MAX_TOTAL_SAMPLES || 1200);
const MAX_REPORT_LAP_DETAIL_DOCS = Number(process.env.POST_SESSION_MAX_LAP_DETAIL_DOCS || 20);
const MAX_REPORT_FINDINGS = Number(process.env.POST_SESSION_MAX_FINDINGS || 8);
const MAX_REPORT_SIGNALS = Number(process.env.POST_SESSION_MAX_SIGNALS || 6);
const MAX_MARKDOWN_LAPS = Number(process.env.POST_SESSION_MAX_MARKDOWN_LAPS || 4);
const MAX_AI_MARKDOWN_CHARS = Number(process.env.POST_SESSION_MAX_MARKDOWN_CHARS || 60000);
const MAX_LIVE_REPORT_ANALYZED_LAPS = Number(process.env.POST_SESSION_LIVE_ANALYZED_LAPS || 4);
const MAX_FINAL_REPORT_ANALYZED_LAPS = Number(process.env.POST_SESSION_FINAL_ANALYZED_LAPS || 10);
const MAX_REPORT_LAP_SUMMARIES = Number(process.env.POST_SESSION_MAX_LAP_SUMMARIES || 40);
const POST_SESSION_LIVE_REPORT_ENABLED = process.env.POST_SESSION_LIVE_REPORT_ENABLED !== "false";
const POST_SESSION_FINAL_REPORT_ENABLED = process.env.POST_SESSION_FINAL_REPORT_ENABLED !== "false";
const POST_SESSION_LIVE_REPORT_DEBOUNCE_MS = Number(process.env.POST_SESSION_LIVE_REPORT_DEBOUNCE_MS || 12000);
const LAP_TRAIL_SPEED_PATCH_APPLIED = true;
const MAP_POINT_CHUNK_PAGE_SIZE = Number(process.env.MAP_POINT_CHUNK_PAGE_SIZE || 25);
const MAX_SESSION_MAP_POINTS = Number(process.env.MAX_SESSION_MAP_POINTS || 5000);
const MAX_LAP_TRAILS = Number(process.env.MAX_LAP_TRAILS || 12);
const APEX_CORNER_DISTANCE_STEP_M = Number(process.env.POST_SESSION_CORNER_STEP_M || 2);
const APEX_CORNER_MIN_GAP_M = Number(process.env.POST_SESSION_CORNER_MIN_GAP_M || 80);
const APEX_CORNER_PROMINENCE_KPH = Number(process.env.POST_SESSION_CORNER_PROMINENCE_KPH || 8);
const APEX_CORNER_WINDOW_BEFORE_M = Number(process.env.POST_SESSION_CORNER_WINDOW_BEFORE_M || 120);
const APEX_CORNER_WINDOW_AFTER_M = Number(process.env.POST_SESSION_CORNER_WINDOW_AFTER_M || 40);
const APEX_CORNER_EXIT_WINDOW_M = Number(process.env.POST_SESSION_CORNER_EXIT_WINDOW_M || 150);
const APEX_CORNER_BRAKE_THRESHOLD = Number(process.env.POST_SESSION_CORNER_BRAKE_THRESHOLD || 0.1);
const APEX_CORNER_FULL_THROTTLE = Number(process.env.POST_SESSION_CORNER_FULL_THROTTLE || 0.95);
function reportRound(value, digits = 2) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function reportAvg(values) {
  const clean = values.filter((value) => Number.isFinite(Number(value))).map(Number);
  if (!clean.length) return null;
  return clean.reduce((sum, value) => sum + value, 0) / clean.length;
}

function reportPercentile(values, percentile) {
  const clean = values.filter((value) => Number.isFinite(Number(value))).map(Number).sort((a, b) => a - b);
  if (!clean.length) return null;
  const index = Math.max(0, Math.min(clean.length - 1, Math.round((percentile / 100) * (clean.length - 1))));
  return clean[index];
}

function reportRatio(samples, predicate) {
  if (!samples.length) return null;
  return (samples.filter(predicate).length / samples.length) * 100;
}

function reportTimestampMs(value) {
  if (!value) return null;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function reportIso(value) {
  if (!value) return null;
  if (typeof value.toDate === "function") return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function reportDurationSec(samples) {
  const times = samples.map((sample) => reportTimestampMs(sample.timestamp)).filter((value) => value !== null);
  if (times.length < 2) return null;
  return Math.max(0, (Math.max(...times) - Math.min(...times)) / 1000);
}

function reportDurationBetween(a, b, fallbackSampleCount = null) {
  const at = reportTimestampMs(a?.timestamp);
  const bt = reportTimestampMs(b?.timestamp);
  if (at !== null && bt !== null) return Math.max(0, (bt - at) / 1000);
  if (fallbackSampleCount !== null) return Math.max(0, fallbackSampleCount * 0.1);
  return null;
}

function reportDistanceSpan(samples, key = "lapDistance") {
  const distances = samples.map((sample) => finiteNumberOrNull(sample[key])).filter((value) => value !== null);
  if (!distances.length) return null;
  return Math.max(...distances) - Math.min(...distances);
}

function reportFormatMs(ms) {
  const value = parseInteger(ms, null);
  if (value === null || value <= 0) return "-";
  const minutes = Math.floor(value / 60000);
  const seconds = Math.floor((value % 60000) / 1000);
  const fraction = value % 1000;
  return minutes + ":" + String(seconds).padStart(2, "0") + "." + String(fraction).padStart(3, "0");
}

function reportFormatSec(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "-";
  const minutes = Math.floor(value / 60);
  const rest = value - minutes * 60;
  return minutes + ":" + rest.toFixed(2).padStart(5, "0");
}

function reportFormatNumber(value, digits = 1, suffix = "") {
  if (value === null || value === undefined) return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits) + suffix;
}

function reportFormatPct(value) {
  return reportFormatNumber(value, 1, "%");
}

function normalizeReportSample(raw, fallbackIndex) {
  return {
    timestamp: safeString(raw?.timestamp, null),
    sampleIndex: parseInteger(raw?.sampleIndex, fallbackIndex),
    gameTimeMs: finiteNumberOrNull(raw?.gameTimeMs),
    lapNumber: parseInteger(raw?.lapNumber, null),
    lapDistance: finiteNumberOrNull(raw?.lapDistance),
    totalDistance: finiteNumberOrNull(raw?.totalDistance),
    worldX: finiteNumberOrNull(raw?.worldX),
    worldY: finiteNumberOrNull(raw?.worldY),
    worldZ: finiteNumberOrNull(raw?.worldZ),
    speedKph: finiteNumberOrNull(raw?.speedKph),
    throttle: finiteNumberOrNull(raw?.throttle),
    brake: finiteNumberOrNull(raw?.brake),
    steering: finiteNumberOrNull(raw?.steering),
    rpm: parseInteger(raw?.rpm, null),
    gear: parseInteger(raw?.gear, null),
    deltaToPB: finiteNumberOrNull(raw?.deltaToPB),
    corneringSpeed: finiteNumberOrNull(raw?.corneringSpeed),
    brakingDistance: finiteNumberOrNull(raw?.brakingDistance),
    drs: parseBoolean(raw?.drs, false),
    currentSector: parseInteger(raw?.currentSector, null),
  };
}

function sortReportSamples(samples) {
  return [...samples].sort((a, b) => {
    const ai = parseInteger(a.sampleIndex, null);
    const bi = parseInteger(b.sampleIndex, null);
    if (ai !== null && bi !== null && ai !== bi) return ai - bi;

    const at = reportTimestampMs(a.timestamp);
    const bt = reportTimestampMs(b.timestamp);
    if (at !== null && bt !== null && at !== bt) return at - bt;

    return 0;
  });
}

function downsampleReportSamples(samples, maxSamples) {
  if (!Array.isArray(samples) || samples.length <= maxSamples) return samples;
  if (maxSamples <= 0) return [];

  const out = [];
  const step = (samples.length - 1) / Math.max(1, maxSamples - 1);

  for (let i = 0; i < maxSamples; i += 1) {
    out.push(samples[Math.round(i * step)]);
  }

  return out;
}

function uniqueSortedLapNumbers(values) {
  return [...new Set((values || [])
    .map((value) => parseInteger(value, null))
    .filter((value) => value !== null && value > 0))]
    .sort((a, b) => a - b);
}

function chooseReportLapNumbers(lapDocs, options = {}) {
  const phase = safeString(options.phase, null) || "live";
  const maxLaps =
    phase === "live"
      ? Math.max(1, MAX_LIVE_REPORT_ANALYZED_LAPS)
      : Math.max(1, MAX_FINAL_REPORT_ANALYZED_LAPS);

  const validTimed = (lapDocs || [])
    .map((lap) => ({
      lapNumber: parseInteger(lap.lapNumber, null),
      lapTimeMs: parseInteger(lap.lapTimeMs, null),
      valid: lap.valid !== false,
    }))
    .filter((lap) => lap.lapNumber !== null);

  const timed = validTimed.filter((lap) => lap.valid && lap.lapTimeMs !== null);
  const byLap = uniqueSortedLapNumbers(validTimed.map((lap) => lap.lapNumber));
  const triggerLap = parseInteger(options.triggerLapNumber, null);
  const chosen = [];

  if (triggerLap !== null) {
    chosen.push(triggerLap - 1, triggerLap);
  }

  if (timed.length) {
    const best = timed.reduce((a, b) => (b.lapTimeMs < a.lapTimeMs ? b : a), timed[0]);
    const worst = timed.reduce((a, b) => (b.lapTimeMs > a.lapTimeMs ? b : a), timed[0]);
    chosen.push(best.lapNumber, worst.lapNumber);
  }

  const recent = byLap.slice(-maxLaps);
  chosen.push(...recent);

  return uniqueSortedLapNumbers(chosen).slice(-maxLaps);
}

function capReportLapNumbers(lapNumbers, options = {}) {
  const triggerLap = parseInteger(options.triggerLapNumber, null);
  if (!Array.isArray(lapNumbers) || lapNumbers.length <= MAX_REPORT_LAP_SUMMARIES) {
    return lapNumbers;
  }

  const chosen = [];
  if (triggerLap !== null) chosen.push(triggerLap - 1, triggerLap);
  chosen.push(...lapNumbers.slice(-MAX_REPORT_LAP_SUMMARIES));
  return uniqueSortedLapNumbers(chosen).slice(-MAX_REPORT_LAP_SUMMARIES);
}

async function collectSessionTelemetrySamples(sessionRef, options = {}) {
  const targetLapNumbers = uniqueSortedLapNumbers(options.targetLapNumbers || []);
  const targetLapSet = targetLapNumbers.length ? new Set(targetLapNumbers.map(String)) : null;
  const maxSamplesPerLap =
    Math.max(20, parseInteger(options.maxSamplesPerLap, MAX_REPORT_SAMPLES_PER_LAP) || MAX_REPORT_SAMPLES_PER_LAP);
  const maxTotalSamples =
    Math.max(100, parseInteger(options.maxTotalSamples, MAX_REPORT_TOTAL_SAMPLES) || MAX_REPORT_TOTAL_SAMPLES);
  const maxAnalyzedLaps =
    Math.max(1, parseInteger(options.maxAnalyzedLaps, targetLapNumbers.length || MAX_FINAL_REPORT_ANALYZED_LAPS) || MAX_FINAL_REPORT_ANALYZED_LAPS);
  const requestedChunkPageSize = parseInteger(options.chunkPageSize, REPORT_TELEMETRY_CHUNK_PAGE_SIZE);
  const chunkPageSize = Math.max(1, Math.min(requestedChunkPageSize || REPORT_TELEMETRY_CHUNK_PAGE_SIZE, 30));

  const samplesByLap = new Map();
  let fallbackIndex = 0;
  let rawSampleCount = 0;
  let lastDoc = null;

  while (true) {
    let queryRef = sessionRef
      .collection("telemetryChunks")
      .orderBy(FieldPath.documentId())
      .limit(chunkPageSize);

    if (lastDoc) {
      queryRef = queryRef.startAfter(lastDoc);
    }

    const chunksSnap = await queryRef.get();
    if (chunksSnap.empty) break;

    for (const doc of chunksSnap.docs) {
      const data = doc.data() || {};
      const chunkSamples = Array.isArray(data.samples) ? data.samples : [];

      for (const sample of chunkSamples) {
        fallbackIndex += 1;
        rawSampleCount += 1;

        const normalized = normalizeReportSample(sample, fallbackIndex);
        const hasDrivingData =
          normalized.speedKph !== null ||
          normalized.throttle !== null ||
          normalized.brake !== null ||
          normalized.steering !== null;

        if (!hasDrivingData) continue;

        const lapKey = normalized.lapNumber === null ? "unknown" : String(normalized.lapNumber);
        if (targetLapSet && !targetLapSet.has(lapKey)) continue;
        if (!targetLapSet && !samplesByLap.has(lapKey) && samplesByLap.size >= maxAnalyzedLaps) continue;

        if (!samplesByLap.has(lapKey)) samplesByLap.set(lapKey, []);

        const bucket = samplesByLap.get(lapKey);
        bucket.push(normalized);

        if (bucket.length > Math.ceil(maxSamplesPerLap * 1.15)) {
          samplesByLap.set(lapKey, downsampleReportSamples(bucket, maxSamplesPerLap));
        }
      }
    }

    lastDoc = chunksSnap.docs[chunksSnap.docs.length - 1];
    if (chunksSnap.size < chunkPageSize) break;
  }

  const samples = [];
  for (const bucket of samplesByLap.values()) {
    samples.push(...downsampleReportSamples(bucket, maxSamplesPerLap));
  }

  const capped = downsampleReportSamples(sortReportSamples(samples), maxTotalSamples);

  Object.defineProperty(capped, "rawSampleCount", {
    value: rawSampleCount,
    enumerable: false,
  });

  return capped;
}

async function collectSessionLapDocs(sessionRef) {
  const snap = await sessionRef.collection("laps").get();
  return snap.docs
    .map((docSnap) => serializeDoc(docSnap))
    .sort((a, b) => (parseInteger(a.lapNumber, 0) || 0) - (parseInteger(b.lapNumber, 0) || 0));
}

async function collectSessionCornerDocs(sessionRef, targetLapNumbers = null) {
  const targetSet = targetLapNumbers?.length
    ? new Set(uniqueSortedLapNumbers(targetLapNumbers).map(String))
    : null;
  const snap = await sessionRef.collection("corners").get();
  return snap.docs
    .map((docSnap) => serializeDoc(docSnap))
    .filter((corner) => {
      if (!targetSet) return true;
      const lapNumber = parseInteger(corner.startLapNumber ?? corner.endLapNumber, null);
      return lapNumber !== null && targetSet.has(String(lapNumber));
    })
    .sort((a, b) => {
      const aLap = parseInteger(a.startLapNumber ?? a.endLapNumber, 0) || 0;
      const bLap = parseInteger(b.startLapNumber ?? b.endLapNumber, 0) || 0;
      if (aLap !== bLap) return aLap - bLap;
      return (parseNumber(a.startLapDistanceM, 0) || 0) - (parseNumber(b.startLapDistanceM, 0) || 0);
    });
}

function reportSegmentRuns(samples, predicate, minPoints = 3) {
  const runs = [];
  let current = [];

  for (const sample of samples) {
    if (predicate(sample)) {
      current.push(sample);
    } else {
      if (current.length >= minPoints) runs.push(current);
      current = [];
    }
  }

  if (current.length >= minPoints) runs.push(current);
  return runs;
}

function reportLongestRun(samples, predicate) {
  let best = [];
  let current = [];

  for (const sample of samples) {
    if (predicate(sample)) {
      current.push(sample);
      if (current.length > best.length) best = [...current];
    } else {
      current = [];
    }
  }

  return best;
}

function reportSliceUntil(samples, predicate) {
  const out = [];
  for (const sample of samples) {
    if (predicate(sample)) break;
    out.push(sample);
  }
  return out;
}

function reportSteeringSmoothness(samples) {
  if (samples.length < 2) return null;
  let totalChange = 0;
  let count = 0;
  for (let i = 1; i < samples.length; i += 1) {
    const prev = finiteNumberOrNull(samples[i - 1].steering);
    const curr = finiteNumberOrNull(samples[i].steering);
    if (prev === null || curr === null) continue;
    totalChange += Math.abs(curr - prev);
    count += 1;
  }
  return count ? totalChange / count : null;
}

function reportAttachLapIndexes(samples) {
  return samples.map((sample, index) => ({
    ...sample,
    lapSampleIndex: index,
  }));
}

function summarizeBrakingZone(run, lapSamples, zoneIndex) {
  const speeds = run.map((sample) => sample.speedKph).filter((value) => value !== null);
  const brakes = run.map((sample) => sample.brake).filter((value) => value !== null);
  const throttles = run.map((sample) => sample.throttle).filter((value) => value !== null);
  const steerings = run.map((sample) => Math.abs(sample.steering ?? 0));
  const startIndex = run[0]?.lapSampleIndex ?? 0;
  const endIndex = run[run.length - 1]?.lapSampleIndex ?? startIndex;
  const before = lapSamples.slice(Math.max(0, startIndex - 6), startIndex);
  const after = lapSamples.slice(endIndex + 1, Math.min(lapSamples.length, endIndex + 26));
  const approachSample = before.length ? before[before.length - 1] : run[0];
  const releaseSample = run[run.length - 1];
  const throttlePickupSample = after.find((sample) => (sample.throttle ?? 0) >= 0.4) || null;
  const firstPositiveThrottleSample = after.find((sample) => (sample.throttle ?? 0) >= 0.1) || null;
  const coastAfterBrakeSamples = reportSliceUntil(after, (sample) => (sample.throttle ?? 0) >= 0.1 || (sample.brake ?? 0) >= 0.05);
  const trailBrakeSamples = run.filter((sample) => Math.abs(sample.steering ?? 0) >= 0.25 && (sample.brake ?? 0) >= 0.05 && (sample.brake ?? 0) <= 0.75);
  const overlapWindow = lapSamples.slice(startIndex, Math.min(lapSamples.length, endIndex + 12));
  const overlapSamples = overlapWindow.filter((sample) => (sample.throttle ?? 0) >= 0.1 && (sample.brake ?? 0) >= 0.1);
  const minSpeed = speeds.length ? Math.min(...speeds) : null;
  const entrySpeed = reportRound(run[0]?.speedKph, 1);
  const approachSpeed = reportRound(approachSample?.speedKph, 1);
  const exitSpeed = reportRound((throttlePickupSample || releaseSample)?.speedKph, 1);
  const speedDrop = approachSpeed !== null && minSpeed !== null ? approachSpeed - minSpeed : null;
  const issueTags = [];

  const coastingAfterBrakeSec = reportRound(reportDurationSec(coastAfterBrakeSamples), 2);
  const throttlePickupDelaySec = throttlePickupSample
    ? reportRound(reportDurationBetween(releaseSample, throttlePickupSample, Math.max(0, throttlePickupSample.lapSampleIndex - releaseSample.lapSampleIndex)), 2)
    : null;
  const overlapDurationSec = reportRound(reportDurationSec(overlapSamples), 2);
  const durationSec = reportRound(reportDurationSec(run), 2);

  if ((coastingAfterBrakeSec ?? 0) >= 0.35) issueTags.push("delayed throttle after brake release");
  if ((overlapDurationSec ?? 0) >= 0.25) issueTags.push("brake/throttle overlap");
  if ((durationSec ?? 0) >= 1.7) issueTags.push("long braking phase");
  if ((speedDrop ?? 0) >= 135) issueTags.push("large speed drop");
  if ((trailBrakeSamples.length / Math.max(1, run.length)) < 0.12 && steerings.some((value) => value >= 0.35)) {
    issueTags.push("little trail braking into steering phase");
  }

  return stripUndefinedDeep({
    zoneId: "BZ-" + String(zoneIndex).padStart(2, "0"),
    zoneType: "braking",
    startLapDistanceM: reportRound(run[0]?.lapDistance, 1),
    endLapDistanceM: reportRound(releaseSample?.lapDistance, 1),
    releaseLapDistanceM: reportRound(releaseSample?.lapDistance, 1),
    throttlePickupLapDistanceM: reportRound(throttlePickupSample?.lapDistance ?? firstPositiveThrottleSample?.lapDistance, 1),
    distanceM: reportRound(reportDistanceSpan(run), 1),
    durationSec,
    approachSpeedKph: reportRound(approachSpeed, 1),
    entrySpeedKph: reportRound(entrySpeed, 1),
    minSpeedKph: reportRound(minSpeed, 1),
    releaseSpeedKph: reportRound(releaseSample?.speedKph, 1),
    throttlePickupSpeedKph: reportRound(throttlePickupSample?.speedKph ?? firstPositiveThrottleSample?.speedKph, 1),
    exitSpeedKph: exitSpeed,
    speedDropKph: reportRound(speedDrop, 1),
    peakBrakePct: reportRound((brakes.length ? Math.max(...brakes) : 0) * 100, 1),
    avgBrakePct: reportRound((reportAvg(brakes) ?? 0) * 100, 1),
    avgThrottlePctDuringBrake: reportRound((reportAvg(throttles) ?? 0) * 100, 1),
    peakAbsSteeringDuringBrake: reportRound(steerings.length ? Math.max(...steerings) : null, 3),
    trailBrakePctOfZone: reportRound((trailBrakeSamples.length / Math.max(1, run.length)) * 100, 1),
    coastingAfterBrakeSec,
    coastingAfterBrakeDistanceM: reportRound(reportDistanceSpan(coastAfterBrakeSamples), 1),
    throttlePickupDelaySec,
    overlapDurationSec,
    issueTags,
  });
}

function summarizeCornerZone(run, lapSamples, zoneIndex) {
  const speeds = run.map((sample) => sample.speedKph).filter((value) => value !== null);
  const throttles = run.map((sample) => sample.throttle).filter((value) => value !== null);
  const brakes = run.map((sample) => sample.brake).filter((value) => value !== null);
  const steerings = run.map((sample) => Math.abs(sample.steering ?? 0));
  const startIndex = run[0]?.lapSampleIndex ?? 0;
  const endIndex = run[run.length - 1]?.lapSampleIndex ?? startIndex;
  const after = lapSamples.slice(endIndex + 1, Math.min(lapSamples.length, endIndex + 18));
  const apexSample = [...run].sort((a, b) => (a.speedKph ?? 9999) - (b.speedKph ?? 9999))[0] || run[Math.floor(run.length / 2)];
  const throttlePickupSample = [...run, ...after].find((sample) => (sample.throttle ?? 0) >= 0.4) || null;
  const overlapSamples = run.filter((sample) => (sample.throttle ?? 0) >= 0.1 && (sample.brake ?? 0) >= 0.1);
  const coastingSamples = run.filter((sample) => (sample.throttle ?? 0) < 0.05 && (sample.brake ?? 0) < 0.05);
  const exitSample = after.find((sample) => (sample.throttle ?? 0) >= 0.4) || run[run.length - 1];
  const speedDrop = run[0]?.speedKph !== null && apexSample?.speedKph !== null ? run[0].speedKph - apexSample.speedKph : null;
  const issueTags = [];
  const coastingPct = reportRound((coastingSamples.length / Math.max(1, run.length)) * 100, 1);
  const overlapDurationSec = reportRound(reportDurationSec(overlapSamples), 2);
  const throttlePickupDelaySec = throttlePickupSample
    ? reportRound(reportDurationBetween(apexSample, throttlePickupSample, Math.max(0, throttlePickupSample.lapSampleIndex - apexSample.lapSampleIndex)), 2)
    : null;
  const peakAbsSteering = steerings.length ? Math.max(...steerings) : null;
  const steeringSmoothness = reportRound(reportSteeringSmoothness(run), 4);

  if ((coastingPct ?? 0) >= 25) issueTags.push("coasting through corner");
  if ((throttlePickupDelaySec ?? 0) >= 0.45) issueTags.push("late throttle pickup after apex");
  if ((overlapDurationSec ?? 0) >= 0.25) issueTags.push("pedal overlap in corner");
  if ((peakAbsSteering ?? 0) >= 0.85) issueTags.push("high steering angle");
  if ((steeringSmoothness ?? 0) >= 0.09) issueTags.push("abrupt steering changes");
  if ((speedDrop ?? 0) >= 85) issueTags.push("large corner speed drop");

  return stripUndefinedDeep({
    zoneId: "CZ-" + String(zoneIndex).padStart(2, "0"),
    zoneType: "cornering",
    startLapDistanceM: reportRound(run[0]?.lapDistance, 1),
    apexLapDistanceM: reportRound(apexSample?.lapDistance, 1),
    endLapDistanceM: reportRound(run[run.length - 1]?.lapDistance, 1),
    throttlePickupLapDistanceM: reportRound(throttlePickupSample?.lapDistance, 1),
    distanceM: reportRound(reportDistanceSpan(run), 1),
    durationSec: reportRound(reportDurationSec(run), 2),
    entrySpeedKph: reportRound(run[0]?.speedKph, 1),
    apexSpeedKph: reportRound(apexSample?.speedKph, 1),
    minSpeedKph: speeds.length ? reportRound(Math.min(...speeds), 1) : null,
    exitSpeedKph: reportRound(exitSample?.speedKph, 1),
    speedDropKph: reportRound(speedDrop, 1),
    avgSpeedKph: reportRound(reportAvg(speeds), 1),
    avgThrottlePct: reportRound((reportAvg(throttles) ?? 0) * 100, 1),
    throttleAtApexPct: reportRound((apexSample?.throttle ?? 0) * 100, 1),
    throttleAtExitPct: reportRound((exitSample?.throttle ?? 0) * 100, 1),
    avgBrakePct: reportRound((reportAvg(brakes) ?? 0) * 100, 1),
    brakeAtApexPct: reportRound((apexSample?.brake ?? 0) * 100, 1),
    peakAbsSteering: reportRound(peakAbsSteering, 3),
    avgAbsSteering: reportRound(reportAvg(steerings), 3),
    steeringSmoothness,
    coastingPct,
    overlapDurationSec,
    throttlePickupDelaySec,
    issueTags,
  });
}

function summarizeReportCornersForLap(corners) {
  return corners.slice(0, 10).map((corner) =>
    stripUndefinedDeep({
      cornerIndex: parseInteger(corner.cornerIndex, null),
      startLapDistanceM: reportRound(parseNumber(corner.startLapDistanceM, null), 1),
      endLapDistanceM: reportRound(parseNumber(corner.endLapDistanceM, null), 1),
      durationMs: parseInteger(corner.durationMs, null),
      startSpeedKph: reportRound(parseNumber(corner.startSpeedKph, null), 1),
      endSpeedKph: reportRound(parseNumber(corner.endSpeedKph, null), 1),
      maxAbsSteering: reportRound(parseNumber(corner.maxAbsSteering, null), 3),
      endReason: safeString(corner.endReason, null),
    })
  );
}

function findReferenceZone(zone, referenceZones, maxDistanceM = 90) {
  if (!zone || !Array.isArray(referenceZones) || !referenceZones.length) return null;
  const start = finiteNumberOrNull(zone.startLapDistanceM);
  if (start === null) return null;

  let best = null;
  let bestDistance = Infinity;

  for (const candidate of referenceZones) {
    const candidateStart = finiteNumberOrNull(candidate.startLapDistanceM);
    if (candidateStart === null) continue;
    const distance = Math.abs(candidateStart - start);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = candidate;
    }
  }

  return best && bestDistance <= maxDistanceM ? best : null;
}

function addZoneComparisons(lapSummaries, bestLapNumber) {
  const bestLap = lapSummaries.find((lap) => lap.lapNumber === bestLapNumber) || null;
  if (!bestLap) return lapSummaries;

  for (const lap of lapSummaries) {
    for (const zone of lap.brakingZones || []) {
      const ref = findReferenceZone(zone, bestLap.brakingZones || []);
      if (!ref || lap.lapNumber === bestLapNumber) continue;
      zone.comparisonToBestLap = stripUndefinedDeep({
        referenceLap: bestLapNumber,
        referenceZoneId: ref.zoneId,
        deltaDurationSec: reportRound((zone.durationSec ?? 0) - (ref.durationSec ?? 0), 2),
        deltaMinSpeedKph: reportRound((zone.minSpeedKph ?? 0) - (ref.minSpeedKph ?? 0), 1),
        deltaExitSpeedKph: reportRound((zone.exitSpeedKph ?? 0) - (ref.exitSpeedKph ?? 0), 1),
        deltaThrottlePickupDelaySec: zone.throttlePickupDelaySec !== null && ref.throttlePickupDelaySec !== null
          ? reportRound(zone.throttlePickupDelaySec - ref.throttlePickupDelaySec, 2)
          : null,
        deltaCoastingAfterBrakeSec: zone.coastingAfterBrakeSec !== null && ref.coastingAfterBrakeSec !== null
          ? reportRound(zone.coastingAfterBrakeSec - ref.coastingAfterBrakeSec, 2)
          : null,
      });
    }

    for (const zone of lap.corneringZones || []) {
      const ref = findReferenceZone(zone, bestLap.corneringZones || []);
      if (!ref || lap.lapNumber === bestLapNumber) continue;
      zone.comparisonToBestLap = stripUndefinedDeep({
        referenceLap: bestLapNumber,
        referenceZoneId: ref.zoneId,
        deltaApexSpeedKph: reportRound((zone.apexSpeedKph ?? 0) - (ref.apexSpeedKph ?? 0), 1),
        deltaExitSpeedKph: reportRound((zone.exitSpeedKph ?? 0) - (ref.exitSpeedKph ?? 0), 1),
        deltaThrottleAtApexPct: reportRound((zone.throttleAtApexPct ?? 0) - (ref.throttleAtApexPct ?? 0), 1),
        deltaCoastingPct: reportRound((zone.coastingPct ?? 0) - (ref.coastingPct ?? 0), 1),
        deltaPeakAbsSteering: reportRound((zone.peakAbsSteering ?? 0) - (ref.peakAbsSteering ?? 0), 3),
      });
    }
  }

  return lapSummaries;
}


function reportControlFraction(value) {
  const n = finiteNumberOrNull(value);
  if (n === null) return null;
  const fraction = n > 1.5 ? n / 100 : n;
  return Math.max(0, Math.min(1, fraction));
}

function prepareApexDistanceTrace(samples) {
  const points = sortReportSamples(samples)
    .map((sample) => ({
      distance: finiteNumberOrNull(sample.lapDistance),
      speedKph: finiteNumberOrNull(sample.speedKph),
      brake: reportControlFraction(sample.brake),
      throttle: reportControlFraction(sample.throttle),
      sampleIndex: parseInteger(sample.sampleIndex, null),
    }))
    .filter((point) => point.distance !== null && point.distance >= 0 && point.speedKph !== null)
    .sort((a, b) => {
      if (a.distance !== b.distance) return a.distance - b.distance;
      return (a.sampleIndex ?? 0) - (b.sampleIndex ?? 0);
    });

  const deduped = [];
  for (const point of points) {
    const previous = deduped[deduped.length - 1];
    if (previous && Math.abs(previous.distance - point.distance) < 0.001) {
      deduped[deduped.length - 1] = point;
    } else if (!previous || point.distance > previous.distance) {
      deduped.push(point);
    }
  }

  return deduped;
}

function interpolateApexSeries(points, key, grid, fallback = null) {
  const series = points
    .map((point) => ({
      distance: point.distance,
      value: finiteNumberOrNull(point[key]),
    }))
    .filter((point) => point.value !== null);

  if (!series.length) return grid.map(() => fallback);
  if (series.length === 1) return grid.map(() => series[0].value);

  const out = [];
  let cursor = 0;

  for (const distance of grid) {
    while (cursor < series.length - 2 && series[cursor + 1].distance < distance) {
      cursor += 1;
    }

    const left = series[cursor];
    const right = series[cursor + 1] || left;

    if (distance <= series[0].distance) {
      out.push(series[0].value);
    } else if (distance >= series[series.length - 1].distance) {
      out.push(series[series.length - 1].value);
    } else if (!right || right.distance === left.distance) {
      out.push(left.value);
    } else {
      const t = (distance - left.distance) / (right.distance - left.distance);
      out.push(left.value + (right.value - left.value) * t);
    }
  }

  return out;
}

function resampleReportLapByDistance(samples, stepM = APEX_CORNER_DISTANCE_STEP_M) {
  const points = prepareApexDistanceTrace(samples);
  if (points.length < 8) return null;

  const startDistance = points[0].distance;
  const endDistance = points[points.length - 1].distance;
  if (!Number.isFinite(startDistance) || !Number.isFinite(endDistance) || endDistance - startDistance < 80) {
    return null;
  }

  const step = Math.max(1, Number(stepM) || APEX_CORNER_DISTANCE_STEP_M);
  const gridStart = Math.ceil(startDistance / step) * step;
  const gridEnd = Math.floor(endDistance / step) * step;
  const distance = [];

  for (let d = gridStart; d <= gridEnd; d += step) {
    distance.push(reportRound(d, 3));
  }

  if (distance.length < 8) return null;

  return {
    distance,
    speed: interpolateApexSeries(points, "speedKph", distance, null),
    brake: interpolateApexSeries(points, "brake", distance, 0),
    throttle: interpolateApexSeries(points, "throttle", distance, 0),
    sourceSampleCount: points.length,
    distanceStartM: reportRound(startDistance, 1),
    distanceEndM: reportRound(endDistance, 1),
    distanceStepM: step,
  };
}

function findApexCornerApexes(resampled, minGapM = APEX_CORNER_MIN_GAP_M, prominenceKph = APEX_CORNER_PROMINENCE_KPH) {
  if (!resampled?.distance?.length || !resampled?.speed?.length) return [];

  const apexes = [];
  const speed = resampled.speed;
  const distance = resampled.distance;

  for (let i = 2; i < speed.length - 2; i += 1) {
    const current = finiteNumberOrNull(speed[i]);
    if (current === null) continue;

    let localMinimum = true;
    for (let offset = -2; offset <= 2; offset += 1) {
      const neighbor = finiteNumberOrNull(speed[i + offset]);
      if (neighbor !== null && current > neighbor) {
        localMinimum = false;
        break;
      }
    }
    if (!localMinimum) continue;

    const before = speed
      .slice(Math.max(0, i - 15), i)
      .map((value) => finiteNumberOrNull(value))
      .filter((value) => value !== null);
    const after = speed
      .slice(i + 1, Math.min(speed.length, i + 16))
      .map((value) => finiteNumberOrNull(value))
      .filter((value) => value !== null);
    if (!before.length || !after.length) continue;

    const localMaxBefore = Math.max(...before);
    const localMaxAfter = Math.max(...after);
    if (Math.min(localMaxBefore, localMaxAfter) - current >= prominenceKph) {
      apexes.push({
        apexDistanceM: finiteNumberOrNull(distance[i]),
        referenceApexSpeedKph: current,
      });
    }
  }

  const merged = [];
  for (const apex of apexes) {
    const previous = merged[merged.length - 1];
    if (previous && apex.apexDistanceM - previous.apexDistanceM < minGapM) {
      if (apex.referenceApexSpeedKph < previous.referenceApexSpeedKph) {
        merged[merged.length - 1] = apex;
      }
    } else {
      merged.push(apex);
    }
  }

  return merged;
}

function extractApexExitSpeed(resampled, apexDistanceM, nextApexDistanceM = null) {
  const searchLimit = Math.min(
    apexDistanceM + APEX_CORNER_EXIT_WINDOW_M,
    nextApexDistanceM !== null ? nextApexDistanceM - 20 : apexDistanceM + APEX_CORNER_EXIT_WINDOW_M
  );
  const indexes = [];

  for (let i = 0; i < resampled.distance.length; i += 1) {
    const d = resampled.distance[i];
    if (d >= apexDistanceM && d <= searchLimit) indexes.push(i);
  }

  if (!indexes.length) return null;

  const fullThrottleIndex = indexes.find((index) => (resampled.throttle[index] ?? 0) >= APEX_CORNER_FULL_THROTTLE);
  const index = fullThrottleIndex ?? indexes[indexes.length - 1];
  const method = fullThrottleIndex === undefined ? "window_edge_fallback" : "full_throttle_point";

  return stripUndefinedDeep({
    exitSpeedKph: reportRound(resampled.speed[index], 1),
    exitDistanceFromApexM: reportRound(resampled.distance[index] - apexDistanceM, 1),
    exitLapDistanceM: reportRound(resampled.distance[index], 1),
    method,
  });
}

function analyzeApexCorner(resampled, referenceCorner, nextReferenceCorner = null) {
  const apexDistanceM = finiteNumberOrNull(referenceCorner?.apexDistanceM);
  if (apexDistanceM === null) return null;

  const windowStart = apexDistanceM - APEX_CORNER_WINDOW_BEFORE_M;
  const windowEnd = apexDistanceM + APEX_CORNER_WINDOW_AFTER_M;
  const indexes = [];

  for (let i = 0; i < resampled.distance.length; i += 1) {
    const d = resampled.distance[i];
    if (d >= windowStart && d <= windowEnd) indexes.push(i);
  }

  if (!indexes.length) return null;

  let minIndex = indexes[0];
  for (const index of indexes) {
    if ((resampled.speed[index] ?? Infinity) < (resampled.speed[minIndex] ?? Infinity)) {
      minIndex = index;
    }
  }

  const brakeIndex = indexes.find(
    (index) => resampled.distance[index] <= apexDistanceM && (resampled.brake[index] ?? 0) >= APEX_CORNER_BRAKE_THRESHOLD
  );
  const brakePointDistance = brakeIndex === undefined ? null : resampled.distance[brakeIndex];
  const nextApexDistanceM = finiteNumberOrNull(nextReferenceCorner?.apexDistanceM);
  const exit = extractApexExitSpeed(resampled, apexDistanceM, nextApexDistanceM);

  return stripUndefinedDeep({
    apexDistanceM: reportRound(apexDistanceM, 1),
    analysisWindowStartM: reportRound(windowStart, 1),
    analysisWindowEndM: reportRound(windowEnd, 1),
    minCornerSpeedKph: reportRound(resampled.speed[minIndex], 1),
    minSpeedLapDistanceM: reportRound(resampled.distance[minIndex], 1),
    brakePointLapDistanceM: reportRound(brakePointDistance, 1),
    brakeDistanceBeforeApexM:
      brakePointDistance !== null ? reportRound(apexDistanceM - brakePointDistance, 1) : null,
    exitSpeedKph: exit?.exitSpeedKph ?? null,
    exitDistanceFromApexM: exit?.exitDistanceFromApexM ?? null,
    exitLapDistanceM: exit?.exitLapDistanceM ?? null,
    exitMeasurementMethod: exit?.method ?? null,
  });
}

function compareApexCorner(userCorner, referenceCorner, cornerIndex, referenceLapNumber) {
  const cornerId = "AC-" + String(cornerIndex).padStart(2, "0");
  if (!userCorner || !referenceCorner) {
    return {
      cornerId,
      cornerIndex,
      status: "missing",
      referenceLapNumber,
      apexDistanceM: referenceCorner?.apexDistanceM ?? null,
    };
  }

  const minSpeedDeltaKph = reportRound(userCorner.minCornerSpeedKph - referenceCorner.minCornerSpeedKph, 1);
  const brakeDistanceDeltaM =
    userCorner.brakeDistanceBeforeApexM !== null && referenceCorner.brakeDistanceBeforeApexM !== null
      ? reportRound(userCorner.brakeDistanceBeforeApexM - referenceCorner.brakeDistanceBeforeApexM, 1)
      : null;
  const exitSpeedDeltaKph =
    userCorner.exitSpeedKph !== null && referenceCorner.exitSpeedKph !== null
      ? reportRound(userCorner.exitSpeedKph - referenceCorner.exitSpeedKph, 1)
      : null;
  const exitDistanceDeltaM =
    userCorner.exitDistanceFromApexM !== null && referenceCorner.exitDistanceFromApexM !== null
      ? reportRound(userCorner.exitDistanceFromApexM - referenceCorner.exitDistanceFromApexM, 1)
      : null;
  const exitMeasurementConfidence =
    userCorner.exitMeasurementMethod === "full_throttle_point" &&
    referenceCorner.exitMeasurementMethod === "full_throttle_point"
      ? "high"
      : "low";
  const issueTags = [];

  if (minSpeedDeltaKph <= -5) issueTags.push("lower minimum corner speed than reference");
  if (brakeDistanceDeltaM !== null && brakeDistanceDeltaM >= 10) issueTags.push("braking earlier than reference");
  if (brakeDistanceDeltaM !== null && brakeDistanceDeltaM <= -10) issueTags.push("braking later than reference");
  if (exitSpeedDeltaKph !== null && exitSpeedDeltaKph <= -5 && exitMeasurementConfidence === "high") {
    issueTags.push("lower exit speed than reference");
  }
  if (exitDistanceDeltaM !== null && exitDistanceDeltaM >= 12 && exitMeasurementConfidence === "high") {
    issueTags.push("reaches full throttle later than reference");
  }
  if (exitMeasurementConfidence === "low") issueTags.push("exit speed confidence low");

  return stripUndefinedDeep({
    cornerId,
    cornerIndex,
    status: "ready",
    referenceLapNumber,
    apexDistanceM: userCorner.apexDistanceM,
    analysisWindowStartM: userCorner.analysisWindowStartM,
    analysisWindowEndM: userCorner.analysisWindowEndM,
    minCornerSpeedKph: userCorner.minCornerSpeedKph,
    brakePointLapDistanceM: userCorner.brakePointLapDistanceM,
    brakeDistanceBeforeApexM: userCorner.brakeDistanceBeforeApexM,
    exitSpeedKph: userCorner.exitSpeedKph,
    exitDistanceFromApexM: userCorner.exitDistanceFromApexM,
    exitMeasurementMethod: userCorner.exitMeasurementMethod,
    reference: {
      minCornerSpeedKph: referenceCorner.minCornerSpeedKph,
      brakeDistanceBeforeApexM: referenceCorner.brakeDistanceBeforeApexM,
      exitSpeedKph: referenceCorner.exitSpeedKph,
      exitDistanceFromApexM: referenceCorner.exitDistanceFromApexM,
      exitMeasurementMethod: referenceCorner.exitMeasurementMethod,
    },
    deltas: {
      minSpeedDeltaKph,
      brakeDistanceDeltaM,
      exitSpeedDeltaKph,
      exitDistanceDeltaM,
    },
    exitMeasurementConfidence,
    issueTags,
  });
}

function summarizeApexCornerComparisons(corners) {
  const ready = (corners || []).filter((corner) => corner.status === "ready");
  const highConfidenceExit = ready.filter((corner) => corner.exitMeasurementConfidence === "high");

  return stripUndefinedDeep({
    cornerCount: ready.length,
    highConfidenceExitCount: highConfidenceExit.length,
    lowConfidenceExitCount: ready.length - highConfidenceExit.length,
    avgMinSpeedDeltaKph: reportRound(reportAvg(ready.map((corner) => corner.deltas?.minSpeedDeltaKph)), 1),
    avgBrakeDistanceDeltaM: reportRound(reportAvg(ready.map((corner) => corner.deltas?.brakeDistanceDeltaM)), 1),
    avgExitSpeedDeltaKph: reportRound(reportAvg(highConfidenceExit.map((corner) => corner.deltas?.exitSpeedDeltaKph)), 1),
    avgExitDistanceDeltaM: reportRound(reportAvg(highConfidenceExit.map((corner) => corner.deltas?.exitDistanceDeltaM)), 1),
    weakestCorners: [...ready]
      .sort((a, b) => scoreApexCornerComparison(b) - scoreApexCornerComparison(a))
      .slice(0, 3)
      .map((corner) => stripUndefinedDeep({
        cornerId: corner.cornerId,
        apexDistanceM: corner.apexDistanceM,
        issueTags: corner.issueTags,
        deltas: corner.deltas,
      })),
  });
}

function buildApexCornerAnalysisForLaps(lapSummaries, samplesByLap, bestLap) {
  if (!bestLap?.lapNumber) {
    return {
      lapSummaries,
      reference: { status: "unavailable", reason: "no valid best lap" },
      summary: { status: "unavailable", reason: "no valid best lap" },
    };
  }

  const referenceSamples = samplesByLap.get(bestLap.lapNumber) || [];
  const referenceResampled = resampleReportLapByDistance(referenceSamples);
  if (!referenceResampled) {
    return {
      lapSummaries,
      reference: { status: "unavailable", reason: "reference lap has insufficient lap-distance telemetry", referenceLapNumber: bestLap.lapNumber },
      summary: { status: "unavailable", reason: "reference lap has insufficient lap-distance telemetry", referenceLapNumber: bestLap.lapNumber },
    };
  }

  const apexes = findApexCornerApexes(referenceResampled);
  if (!apexes.length) {
    return {
      lapSummaries,
      reference: { status: "unavailable", reason: "no clear speed-minimum corner apexes found", referenceLapNumber: bestLap.lapNumber },
      summary: { status: "unavailable", reason: "no clear speed-minimum corner apexes found", referenceLapNumber: bestLap.lapNumber },
    };
  }

  const referenceCorners = apexes
    .map((apex, index) => {
      const analyzed = analyzeApexCorner(referenceResampled, apex, apexes[index + 1] || null);
      if (!analyzed) return null;
      return stripUndefinedDeep({
        cornerId: "AC-" + String(index + 1).padStart(2, "0"),
        cornerIndex: index + 1,
        ...analyzed,
      });
    })
    .filter(Boolean);

  if (!referenceCorners.length) {
    return {
      lapSummaries,
      reference: { status: "unavailable", reason: "corner apexes were found but could not be measured", referenceLapNumber: bestLap.lapNumber },
      summary: { status: "unavailable", reason: "corner apexes were found but could not be measured", referenceLapNumber: bestLap.lapNumber },
    };
  }

  const nextLapSummaries = lapSummaries.map((lap) => {
    const lapSamples = samplesByLap.get(lap.lapNumber) || [];
    const resampled = resampleReportLapByDistance(lapSamples);

    if (!resampled) {
      return {
        ...lap,
        apexCornerAnalysis: {
          status: "unavailable",
          reason: "insufficient lap-distance telemetry for this lap",
          referenceLapNumber: bestLap.lapNumber,
          referenceCornerCount: referenceCorners.length,
        },
      };
    }

    const corners = referenceCorners.map((referenceCorner, index) => {
      const userCorner = analyzeApexCorner(resampled, referenceCorner, referenceCorners[index + 1] || null);
      return compareApexCorner(userCorner, referenceCorner, index + 1, bestLap.lapNumber);
    });

    return stripUndefinedDeep({
      ...lap,
      apexCornerAnalysis: {
        status: "ready",
        schema: "distance-apex-corner-v1",
        method:
          "Corners are detected as speed minima on the best actual lap, then every lap is compared on a shared lap-distance grid.",
        referenceLapNumber: bestLap.lapNumber,
        referenceCornerCount: referenceCorners.length,
        analysedCornerCount: corners.filter((corner) => corner.status === "ready").length,
        distanceStepM: resampled.distanceStepM,
        sourceSampleCount: resampled.sourceSampleCount,
        summary: summarizeApexCornerComparisons(corners),
        corners,
      },
    });
  });

  return {
    lapSummaries: nextLapSummaries,
    reference: {
      status: "ready",
      schema: "distance-apex-corner-reference-v1",
      method: "Speed-minimum apexes detected from the best actual lap and reused for lap-to-lap comparison.",
      referenceLapNumber: bestLap.lapNumber,
      cornerCount: referenceCorners.length,
      distanceStepM: referenceResampled.distanceStepM,
      sourceSampleCount: referenceResampled.sourceSampleCount,
      corners: referenceCorners,
    },
    summary: {
      status: "ready",
      referenceLapNumber: bestLap.lapNumber,
      cornerCount: referenceCorners.length,
      distanceStepM: referenceResampled.distanceStepM,
      analysedLapCount: nextLapSummaries.filter((lap) => lap.apexCornerAnalysis?.status === "ready").length,
    },
  };
}

function scoreApexCornerComparison(corner) {
  if (!corner || corner.status !== "ready") return 0;
  const deltas = corner.deltas || {};
  let score = 0;

  if ((deltas.minSpeedDeltaKph ?? 0) <= -5) score += 3;
  if ((deltas.minSpeedDeltaKph ?? 0) <= -10) score += 2;
  if (deltas.brakeDistanceDeltaM != null && deltas.brakeDistanceDeltaM >= 10) score += 2;
  if (deltas.brakeDistanceDeltaM != null && deltas.brakeDistanceDeltaM >= 20) score += 1;
  if (corner.exitMeasurementConfidence === "high" && (deltas.exitSpeedDeltaKph ?? 0) <= -5) score += 3;
  if (corner.exitMeasurementConfidence === "high" && (deltas.exitSpeedDeltaKph ?? 0) <= -10) score += 2;
  if (corner.exitMeasurementConfidence === "high" && (deltas.exitDistanceDeltaM ?? 0) >= 12) score += 3;

  return score;
}

function apexCornerEvidenceText(corner) {
  const deltas = corner.deltas || {};
  return (
    "Apex " + reportFormatNumber(corner.apexDistanceM, 1, " m") +
    ", min speed " + reportFormatNumber(corner.minCornerSpeedKph, 1, " kph") +
    " (" + reportFormatNumber(deltas.minSpeedDeltaKph, 1, " kph vs ref") + ")" +
    ", brake point " + reportFormatNumber(corner.brakeDistanceBeforeApexM, 1, " m before apex") +
    (deltas.brakeDistanceDeltaM != null ? " (" + reportFormatNumber(deltas.brakeDistanceDeltaM, 1, " m vs ref") + ")" : "") +
    ", exit " + reportFormatNumber(corner.exitSpeedKph, 1, " kph") +
    (corner.exitMeasurementConfidence === "high" && deltas.exitSpeedDeltaKph != null
      ? " (" + reportFormatNumber(deltas.exitSpeedDeltaKph, 1, " kph vs ref") + ")"
      : " (exit confidence low)") +
    ", full throttle distance " + reportFormatNumber(corner.exitDistanceFromApexM, 1, " m after apex")
  );
}

function apexCornerCoachingTip(corner) {
  const deltas = corner.deltas || {};
  if (corner.exitMeasurementConfidence === "high" && (deltas.exitDistanceDeltaM ?? 0) >= 12) {
    return "Focus on earlier throttle commitment after the apex. The reference reaches full throttle sooner, so check entry rotation and avoid waiting on exit.";
  }
  if (corner.exitMeasurementConfidence === "high" && (deltas.exitSpeedDeltaKph ?? 0) <= -5) {
    return "Prioritize exit speed here. Try a cleaner brake release and earlier throttle pickup while keeping the car inside track limits.";
  }
  if ((deltas.brakeDistanceDeltaM ?? 0) >= 10 && (deltas.minSpeedDeltaKph ?? 0) <= -5) {
    return "The driver is braking earlier and still carrying less minimum speed. Move the brake point later in small steps and release pressure more progressively.";
  }
  if ((deltas.minSpeedDeltaKph ?? 0) <= -5) {
    return "Carry a little more minimum speed at the apex, but only if it does not delay throttle on exit.";
  }
  return "Use this corner as a reference check for brake point, apex speed, and throttle commitment.";
}

function summarizeReportLap(lapNumber, rawSamples, lapDoc, corners) {
  const samples = reportAttachLapIndexes(rawSamples);
  const speeds = samples.map((sample) => sample.speedKph).filter((value) => value !== null);
  const throttles = samples.map((sample) => sample.throttle).filter((value) => value !== null);
  const brakes = samples.map((sample) => sample.brake).filter((value) => value !== null);
  const steerings = samples.map((sample) => sample.steering).filter((value) => value !== null);
  const deltas = samples.map((sample) => sample.deltaToPB).filter((value) => value !== null);
  const brakingRuns = reportSegmentRuns(samples, (sample) => (sample.brake ?? 0) >= 0.08);
  const cornerRuns = reportSegmentRuns(samples, (sample) => Math.abs(sample.steering ?? 0) >= 0.25);
  const overlapRun = reportLongestRun(samples, (sample) => (sample.throttle ?? 0) >= 0.1 && (sample.brake ?? 0) >= 0.1);
  const coastRun = reportLongestRun(samples, (sample) => (sample.throttle ?? 0) < 0.05 && (sample.brake ?? 0) < 0.05);

  return stripUndefinedDeep({
    lapNumber,
    sampleCount: samples.length,
    lapTimeMs: parseInteger(lapDoc?.lapTimeMs, null),
    lapTime: reportFormatMs(lapDoc?.lapTimeMs),
    sector1Ms: parseInteger(lapDoc?.sector1Ms, null),
    sector2Ms: parseInteger(lapDoc?.sector2Ms, null),
    sector3Ms: parseInteger(lapDoc?.sector3Ms, null),
    valid: lapDoc?.valid !== false,
    approxDurationSec: reportRound(reportDurationSec(samples), 2),
    distanceCoveredM: reportRound(reportDistanceSpan(samples), 1),
    avgSpeedKph: reportRound(reportAvg(speeds), 1),
    maxSpeedKph: speeds.length ? reportRound(Math.max(...speeds), 1) : null,
    p95SpeedKph: reportRound(reportPercentile(speeds, 95), 1),
    avgThrottlePct: reportRound((reportAvg(throttles) ?? 0) * 100, 1),
    fullThrottlePct: reportRound(reportRatio(samples, (sample) => (sample.throttle ?? 0) >= 0.95), 1),
    avgBrakePct: reportRound((reportAvg(brakes) ?? 0) * 100, 1),
    heavyBrakePct: reportRound(reportRatio(samples, (sample) => (sample.brake ?? 0) >= 0.5), 1),
    coastingPct: reportRound(reportRatio(samples, (sample) => (sample.throttle ?? 0) < 0.05 && (sample.brake ?? 0) < 0.05), 1),
    throttleBrakeOverlapPct: reportRound(reportRatio(samples, (sample) => (sample.throttle ?? 0) >= 0.1 && (sample.brake ?? 0) >= 0.1), 1),
    longestThrottleBrakeOverlapSec: reportRound(reportDurationSec(overlapRun), 2),
    longestCoastSec: reportRound(reportDurationSec(coastRun), 2),
    avgAbsSteering: reportRound(reportAvg(steerings.map((value) => Math.abs(value))), 3),
    maxAbsSteering: steerings.length ? reportRound(Math.max(...steerings.map((value) => Math.abs(value))), 3) : null,
    steeringSmoothness: reportRound(reportSteeringSmoothness(samples), 4),
    drsPct: reportRound(reportRatio(samples, (sample) => sample.drs === true), 1),
    bestDeltaToPbMs: deltas.length ? reportRound(Math.min(...deltas), 1) : null,
    finalDeltaToPbMs: deltas.length ? reportRound(deltas[deltas.length - 1], 1) : null,
    brakingZoneCount: brakingRuns.length,
    corneringZoneCount: cornerRuns.length,
    brakingZones: brakingRuns.slice(0, 12).map((run, index) => summarizeBrakingZone(run, samples, index + 1)),
    corneringZones: cornerRuns.slice(0, 12).map((run, index) => summarizeCornerZone(run, samples, index + 1)),
    savedCorners: summarizeReportCornersForLap(corners),
  });
}

function buildDataQuality(samples, laps, corners) {
  const timedLaps = laps.filter((lap) => parseInteger(lap.lapTimeMs, null) !== null);
  const validTimedLaps = timedLaps.filter((lap) => lap.valid !== false);
  const sectorRows = timedLaps.filter((lap) => lap.sector1Ms != null || lap.sector2Ms != null || lap.sector3Ms != null);
  const hasWorld = samples.some((sample) => sample.worldX !== null && sample.worldZ !== null);
  const pedalSamples = samples.filter((sample) => sample.throttle !== null && sample.brake !== null).length;
  const steeringSamples = samples.filter((sample) => sample.steering !== null).length;
  const distanceSamples = samples.filter((sample) => sample.lapDistance !== null).length;
  let confidence = "low";

  if (samples.length >= 300 && validTimedLaps.length >= 1 && pedalSamples / Math.max(1, samples.length) >= 0.8) {
    confidence = "high";
  } else if (samples.length >= 80 && timedLaps.length >= 1) {
    confidence = "medium";
  }

  return {
    confidence,
    sampleCount: samples.length,
    timedLapCount: timedLaps.length,
    validTimedLapCount: validTimedLaps.length,
    sectorLapCount: sectorRows.length,
    cornerEventCount: corners.length,
    pedalCoveragePct: reportRound((pedalSamples / Math.max(1, samples.length)) * 100, 1),
    steeringCoveragePct: reportRound((steeringSamples / Math.max(1, samples.length)) * 100, 1),
    lapDistanceCoveragePct: reportRound((distanceSamples / Math.max(1, samples.length)) * 100, 1),
    worldPositionAvailable: hasWorld,
    limitations: [
      ...(samples.length < 80 ? ["low telemetry sample count"] : []),
      ...(timedLaps.length === 0 ? ["no official lap timing rows"] : []),
      ...(sectorRows.length === 0 ? ["sector timing unavailable"] : []),
      ...(distanceSamples === 0 ? ["lap distance unavailable, zone positions less precise"] : []),
      ...(!hasWorld ? ["world position unavailable, map/corner matching is distance-based only"] : []),
    ],
  };
}

function buildTheoreticalBestLap(lapDocs, bestLap = null) {
  const sectorDefs = [
    { sector: 1, key: "sector1Ms", label: "Sector 1" },
    { sector: 2, key: "sector2Ms", label: "Sector 2" },
    { sector: 3, key: "sector3Ms", label: "Sector 3" },
  ];

  const validRows = (lapDocs || [])
    .map((lap) => ({
      lapNumber: parseInteger(lap.lapNumber, null),
      valid: lap.valid !== false,
      sector1Ms: parseInteger(lap.sector1Ms, null),
      sector2Ms: parseInteger(lap.sector2Ms, null),
      sector3Ms: parseInteger(lap.sector3Ms, null),
      lapTimeMs: parseInteger(lap.lapTimeMs, null),
    }))
    .filter((lap) => lap.valid && lap.lapNumber !== null);

  const sectors = sectorDefs.map((def) => {
    const candidates = validRows
      .map((lap) => ({
        sector: def.sector,
        label: def.label,
        lapNumber: lap.lapNumber,
        sectorTimeMs: parseInteger(lap[def.key], null),
      }))
      .filter((row) => row.sectorTimeMs !== null && row.sectorTimeMs > 0);

    if (!candidates.length) {
      return {
        sector: def.sector,
        label: def.label,
        lapNumber: null,
        sectorTimeMs: null,
        sectorTime: null,
      };
    }

    const best = candidates.reduce(
      (currentBest, row) =>
        row.sectorTimeMs < currentBest.sectorTimeMs ? row : currentBest,
      candidates[0]
    );

    return {
      ...best,
      sectorTime: reportFormatMs(best.sectorTimeMs),
    };
  });

  const isComplete = sectors.every((sector) => sector.sectorTimeMs !== null);
  const lapTimeMs = isComplete
    ? sectors.reduce((sum, sector) => sum + sector.sectorTimeMs, 0)
    : null;
  const bestActualLapTimeMs = parseInteger(bestLap?.lapTimeMs, null);

  return stripUndefinedDeep({
    method:
      "Best valid Sector 1 + best valid Sector 2 + best valid Sector 3. This is not a real driven lap; it is the combined sector pace ceiling from this session.",
    isComplete,
    validSectorLapCount: validRows.length,
    missingSectors: sectors
      .filter((sector) => sector.sectorTimeMs === null)
      .map((sector) => sector.label),
    lapTimeMs,
    lapTime: reportFormatMs(lapTimeMs),
    bestActualLapNumber: bestLap?.lapNumber ?? null,
    bestActualLapTimeMs,
    bestActualGapToTheoreticalMs:
      lapTimeMs !== null && bestActualLapTimeMs !== null
        ? bestActualLapTimeMs - lapTimeMs
        : null,
    sectors,
  });
}

function buildLapComparisonTable(lapSummaries, bestLap, theoreticalBestLap = null) {
  return lapSummaries.map((lap) => ({
    lapNumber: lap.lapNumber,
    valid: lap.valid !== false,
    lapTimeMs: lap.lapTimeMs,
    lapTime: lap.lapTime,
    gapToBestMs: bestLap && lap.lapTimeMs !== null ? lap.lapTimeMs - bestLap.lapTimeMs : null,
    gapToTheoreticalBestMs:
      theoreticalBestLap?.lapTimeMs !== null && lap.lapTimeMs !== null
        ? lap.lapTimeMs - theoreticalBestLap.lapTimeMs
        : null,
    avgSpeedKph: lap.avgSpeedKph,
    maxSpeedKph: lap.maxSpeedKph,
    fullThrottlePct: lap.fullThrottlePct,
    heavyBrakePct: lap.heavyBrakePct,
    coastingPct: lap.coastingPct,
    throttleBrakeOverlapPct: lap.throttleBrakeOverlapPct,
    avgAbsSteering: lap.avgAbsSteering,
    brakingZoneCount: lap.brakingZoneCount,
    corneringZoneCount: lap.corneringZoneCount,
    apexCornerCount: lap.apexCornerAnalysis?.summary?.cornerCount ?? null,
    avgApexMinSpeedDeltaKph: lap.apexCornerAnalysis?.summary?.avgMinSpeedDeltaKph ?? null,
    avgApexBrakeDistanceDeltaM: lap.apexCornerAnalysis?.summary?.avgBrakeDistanceDeltaM ?? null,
    avgApexExitSpeedDeltaKph: lap.apexCornerAnalysis?.summary?.avgExitSpeedDeltaKph ?? null,
  }));
}

function scoreBrakingZone(zone) {
  let score = 0;
  if ((zone.coastingAfterBrakeSec ?? 0) >= 0.35) score += 3;
  if ((zone.overlapDurationSec ?? 0) >= 0.25) score += 3;
  if ((zone.comparisonToBestLap?.deltaExitSpeedKph ?? 0) <= -8) score += 4;
  if ((zone.comparisonToBestLap?.deltaMinSpeedKph ?? 0) <= -8) score += 3;
  if ((zone.comparisonToBestLap?.deltaDurationSec ?? 0) >= 0.3) score += 2;
  if ((zone.speedDropKph ?? 0) >= 135) score += 1;
  return score;
}

function scoreCornerZone(zone) {
  let score = 0;
  if ((zone.coastingPct ?? 0) >= 25) score += 3;
  if ((zone.throttlePickupDelaySec ?? 0) >= 0.45) score += 3;
  if ((zone.comparisonToBestLap?.deltaExitSpeedKph ?? 0) <= -8) score += 4;
  if ((zone.comparisonToBestLap?.deltaApexSpeedKph ?? 0) <= -8) score += 3;
  if ((zone.peakAbsSteering ?? 0) >= 0.85) score += 1;
  if ((zone.steeringSmoothness ?? 0) >= 0.09) score += 1;
  return score;
}

function buildPrecisionFindings(lapSummaries, bestLap) {
  const findings = [];

  for (const lap of lapSummaries) {
    for (const zone of lap.brakingZones || []) {
      const score = scoreBrakingZone(zone);
      if (!score && !(zone.issueTags || []).length) continue;
      findings.push({
        type: "braking",
        severity: score >= 7 ? "high" : score >= 4 ? "medium" : "low",
        score,
        lapNumber: lap.lapNumber,
        zoneId: zone.zoneId,
        location: reportFormatNumber(zone.startLapDistanceM, 1, " m") + " to " + reportFormatNumber(zone.endLapDistanceM, 1, " m"),
        evidence:
          "Entry " + reportFormatNumber(zone.entrySpeedKph, 0, " kph") +
          ", min " + reportFormatNumber(zone.minSpeedKph, 0, " kph") +
          ", exit " + reportFormatNumber(zone.exitSpeedKph, 0, " kph") +
          ", peak brake " + reportFormatPct(zone.peakBrakePct) +
          ", coast after brake " + reportFormatNumber(zone.coastingAfterBrakeSec, 2, " sec") +
          (zone.comparisonToBestLap?.deltaExitSpeedKph != null ? ", exit vs best " + reportFormatNumber(zone.comparisonToBestLap.deltaExitSpeedKph, 1, " kph") : ""),
        interpretation: (zone.issueTags || []).join(", ") || "braking zone differs from best-lap reference",
        coachingTip:
          (zone.comparisonToBestLap?.deltaExitSpeedKph ?? 0) <= -8
            ? "Prioritize exit speed here: release the brake earlier/smoother and pick up throttle sooner after rotation."
            : (zone.coastingAfterBrakeSec ?? 0) >= 0.35
              ? "Reduce the dead time after brake release. Aim for a smoother brake release directly into maintenance throttle."
              : "Review braking pressure and release shape; avoid holding peak brake longer than needed.",
      });
    }

    for (const zone of lap.corneringZones || []) {
      const score = scoreCornerZone(zone);
      if (!score && !(zone.issueTags || []).length) continue;
      findings.push({
        type: "cornering",
        severity: score >= 7 ? "high" : score >= 4 ? "medium" : "low",
        score,
        lapNumber: lap.lapNumber,
        zoneId: zone.zoneId,
        location: reportFormatNumber(zone.startLapDistanceM, 1, " m") + " to " + reportFormatNumber(zone.endLapDistanceM, 1, " m"),
        evidence:
          "Apex " + reportFormatNumber(zone.apexSpeedKph, 0, " kph") +
          ", exit " + reportFormatNumber(zone.exitSpeedKph, 0, " kph") +
          ", throttle at apex " + reportFormatPct(zone.throttleAtApexPct) +
          ", coasting " + reportFormatPct(zone.coastingPct) +
          (zone.comparisonToBestLap?.deltaExitSpeedKph != null ? ", exit vs best " + reportFormatNumber(zone.comparisonToBestLap.deltaExitSpeedKph, 1, " kph") : ""),
        interpretation: (zone.issueTags || []).join(", ") || "corner phase differs from best-lap reference",
        coachingTip:
          (zone.throttlePickupDelaySec ?? 0) >= 0.45 || (zone.coastingPct ?? 0) >= 25
            ? "Work on earlier throttle commitment after apex. If the car pushes wide, slow the entry slightly less abruptly and rotate before throttle."
            : (zone.peakAbsSteering ?? 0) >= 0.85
              ? "Smooth the steering trace and avoid adding extra lock while asking for throttle."
              : "Compare this corner to the best lap and aim to keep minimum speed and exit throttle closer to the reference.",
      });
    }
    for (const corner of lap.apexCornerAnalysis?.corners || []) {
      const score = scoreApexCornerComparison(corner);
      if (!score && !(corner.issueTags || []).length) continue;
      findings.push({
        type: "cornering",
        severity: score >= 7 ? "high" : score >= 4 ? "medium" : "low",
        score,
        lapNumber: lap.lapNumber,
        zoneId: corner.cornerId,
        location: "apex " + reportFormatNumber(corner.apexDistanceM, 1, " m"),
        evidence: apexCornerEvidenceText(corner),
        interpretation: (corner.issueTags || []).join(", ") || "distance-aligned corner metrics differ from reference lap",
        coachingTip: apexCornerCoachingTip(corner),
      });
    }
  }

  return findings.sort((a, b) => b.score - a.score).slice(0, 16);
}

function buildPostSessionCoachSignals(lapSummaries, precisionFindings, bestLap) {
  const signals = [];

  function add(severity, area, lap, evidence, coachingAngle, linkedZone = null) {
    signals.push(stripUndefinedDeep({ severity, area, lap, evidence, coachingAngle, linkedZone }));
  }

  for (const finding of precisionFindings.slice(0, 10)) {
    add(
      finding.severity,
      finding.type === "braking" ? "braking technique" : "corner exit technique",
      finding.lapNumber,
      finding.zoneId + " at " + finding.location + ": " + finding.evidence + ". " + finding.interpretation + ".",
      finding.coachingTip,
      finding.zoneId
    );
  }

  for (const lap of lapSummaries) {
    const label = "Lap " + lap.lapNumber;

    if ((lap.throttleBrakeOverlapPct ?? 0) >= 3) {
      add(
        "high",
        "pedal overlap",
        lap.lapNumber,
        label + ": throttle and brake overlap for " + reportFormatPct(lap.throttleBrakeOverlapPct) + " of telemetry samples.",
        "Avoid carrying throttle while still braking unless it is deliberate stabilization. It usually costs exit speed and heats tyres."
      );
    }

    if ((lap.coastingPct ?? 0) >= 18) {
      add(
        "medium",
        "coasting",
        lap.lapNumber,
        label + ": coasting for " + reportFormatPct(lap.coastingPct) + " of telemetry samples.",
        "Find places where brake release is not followed by throttle. Coach the driver to connect brake release, rotation, and throttle pickup."
      );
    }

    if ((lap.fullThrottlePct ?? 100) < 35 && lap.sampleCount > 15) {
      add(
        "medium",
        "throttle confidence",
        lap.lapNumber,
        label + ": full throttle for only " + reportFormatPct(lap.fullThrottlePct) + " of telemetry samples.",
        "Review exits and straights. The driver may be waiting too long before committing to power."
      );
    }

    if ((lap.heavyBrakePct ?? 0) >= 16) {
      add(
        "medium",
        "braking load",
        lap.lapNumber,
        label + ": heavy braking for " + reportFormatPct(lap.heavyBrakePct) + " of telemetry samples.",
        "Review whether the driver is holding high brake pressure too long instead of releasing smoothly into the corner."
      );
    }
  }

  const unique = [];
  const seen = new Set();
  for (const signal of signals) {
    const key = signal.area + "|" + signal.lap + "|" + (signal.linkedZone || signal.evidence);
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(signal);
  }

  return unique.slice(0, 18);
}

function renderPostSessionMarkdown(report) {
  const s = report.sessionSnapshot;
  const lines = [
    "# F1 25 Coach Evidence Report",
    "",
    "This report is optimized for an AI racing coach. It separates measured evidence from coaching interpretation and includes braking/cornering zone detail.",
    "",
    "## 1. AI Instructions",
    "",
    "- Use only this report as evidence.",
    "- Separate facts from guesses.",
    "- Prefer advice backed by braking-zone, corner-zone, or apex-corner comparison evidence.",
    "- For apex-corner evidence, brakeDistanceDeltaM > 0 means the driver braked earlier than the reference; exitDistanceDeltaM > 0 means they reached full throttle later.",
    "- Only make confident exit-speed claims when exitMeasurementConfidence is high.",
    "- Use theoretical best lap as the sector-combination pace ceiling, but do not treat it as a real driven lap.",
    "- If data quality is limited, lower confidence instead of overclaiming.",
    "",
    "## 2. Data Quality",
    "",
    "- Confidence: " + report.dataQuality.confidence,
    "- Samples recorded: " + report.dataQuality.sampleCount,
    "- Timed laps: " + report.dataQuality.timedLapCount,
    "- Valid timed laps: " + report.dataQuality.validTimedLapCount,
    "- Sector laps: " + report.dataQuality.sectorLapCount,
    "- Pedal coverage: " + reportFormatPct(report.dataQuality.pedalCoveragePct),
    "- Steering coverage: " + reportFormatPct(report.dataQuality.steeringCoveragePct),
    "- Lap-distance coverage: " + reportFormatPct(report.dataQuality.lapDistanceCoveragePct),
    "- World position available: " + (report.dataQuality.worldPositionAvailable ? "yes" : "no"),
    "- Limitations: " + (report.dataQuality.limitations.length ? report.dataQuality.limitations.join("; ") : "none detected"),
    "",
    "## 3. Session Context",
    "",
    "- Session ID: " + s.sessionId,
    "- Driver: " + (s.username || "-"),
    "- Track: " + (s.trackName || "-"),
    "- Session type: " + (s.sessionType ?? "-"),
    "- Started: " + (s.startedAt || "-"),
    "- Ended: " + (s.endedAt || "-"),
    "",
    "## 4. Session Summary",
    "",
    "- Best actual lap: " + (report.bestLap?.lapTime || "-") + (report.bestLap ? " on lap " + report.bestLap.lapNumber : ""),
    "- Theoretical best lap: " + (report.theoreticalBestLap?.lapTime || "-") + (report.theoreticalBestLap?.isComplete ? " (best valid sectors combined)" : " (sector data incomplete)"),
    "- Best actual gap to theoretical: " + (report.theoreticalBestLap?.bestActualGapToTheoreticalMs != null ? reportFormatNumber(report.theoreticalBestLap.bestActualGapToTheoreticalMs / 1000, 3, " sec") : "-"),
    "- Theoretical sector sources: " + (report.theoreticalBestLap?.sectors?.length ? report.theoreticalBestLap.sectors.map((sector) => sector.sectorTime ? sector.label + " " + sector.sectorTime + " from lap " + sector.lapNumber : sector.label + " unavailable").join("; ") : "-"),
    "- Apex corner reference: " + (report.apexCornerAnalysisSummary?.status === "ready" ? String(report.apexCornerAnalysisSummary.cornerCount) + " corners from lap " + report.apexCornerAnalysisSummary.referenceLapNumber : (report.apexCornerAnalysisSummary?.reason || "unavailable")),
    "- Worst valid lap: " + (report.worstLap?.lapTime || "-") + (report.worstLap ? " on lap " + report.worstLap.lapNumber : ""),
    "- Top speed: " + reportFormatNumber(s.topSpeedKph, 0, " kph"),
    "- Average speed: " + reportFormatNumber(s.avgSpeedKph, 1, " kph"),
    "- Main automatic focus: " + (report.precisionFindings[0]?.interpretation || "no strong weakness detected"),
    "",
    "## 5. Lap Comparison",
    "",
    "| Lap | Time | Gap To Best Actual | Gap To Theoretical | Avg Speed | Top Speed | Full Throttle | Heavy Brake | Coasting | Pedal Overlap |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  ];

  for (const row of report.lapComparisonTable) {
    lines.push(
      "| " +
        [
          row.lapNumber,
          row.lapTime || "-",
          row.gapToBestMs === null ? "-" : reportFormatNumber(row.gapToBestMs / 1000, 3, " sec"),
          row.gapToTheoreticalBestMs === null ? "-" : reportFormatNumber(row.gapToTheoreticalBestMs / 1000, 3, " sec"),
          reportFormatNumber(row.avgSpeedKph, 1, " kph"),
          reportFormatNumber(row.maxSpeedKph, 0, " kph"),
          reportFormatPct(row.fullThrottlePct),
          reportFormatPct(row.heavyBrakePct),
          reportFormatPct(row.coastingPct),
          reportFormatPct(row.throttleBrakeOverlapPct),
        ].join(" | ") +
        " |"
    );
  }

  lines.push("", "## 6. Top Coach Signals", "");

  if (!report.topCoachSignals.length) {
    lines.push("No major automatic warning signals were detected. Focus on lap-to-lap consistency and racing line.");
  } else {
    report.topCoachSignals.forEach((signal, index) => {
      lines.push(
        "### Signal " + (index + 1) + ": " + signal.area,
        "",
        "- Severity: " + signal.severity,
        "- Lap: " + signal.lap,
        "- Evidence: " + signal.evidence,
        "- Coaching angle: " + signal.coachingAngle,
        ""
      );
    });
  }

  lines.push(
    "## 7. Precision Braking Findings",
    "",
    "| Lap | Zone | Location | Severity | Evidence | Coaching Tip |",
    "| --- | --- | --- | --- | --- | --- |"
  );

  for (const finding of report.precisionFindings.filter((item) => item.type === "braking").slice(0, 8)) {
    lines.push(
      "| " +
        [
          finding.lapNumber,
          finding.zoneId,
          finding.location,
          finding.severity,
          finding.evidence,
          finding.coachingTip,
        ].join(" | ") +
        " |"
    );
  }

  lines.push(
    "",
    "## 8. Precision Corner Findings",
    "",
    "| Lap | Zone | Location | Severity | Evidence | Coaching Tip |",
    "| --- | --- | --- | --- | --- | --- |"
  );

  for (const finding of report.precisionFindings.filter((item) => item.type === "cornering").slice(0, 8)) {
    lines.push(
      "| " +
        [
          finding.lapNumber,
          finding.zoneId,
          finding.location,
          finding.severity,
          finding.evidence,
          finding.coachingTip,
        ].join(" | ") +
        " |"
    );
  }

  lines.push("", "## 9. Lap Details", "");

  for (const lap of report.lapSummaries) {
    lines.push(
      "### Lap " + lap.lapNumber,
      "",
      "- Lap time: " + (lap.lapTime || "-"),
      "- Sector 1: " + reportFormatMs(lap.sector1Ms),
      "- Sector 2: " + reportFormatMs(lap.sector2Ms),
      "- Sector 3: " + reportFormatMs(lap.sector3Ms),
      "- Full throttle: " + reportFormatPct(lap.fullThrottlePct),
      "- Heavy brake: " + reportFormatPct(lap.heavyBrakePct),
      "- Coasting: " + reportFormatPct(lap.coastingPct),
      "- Pedal overlap: " + reportFormatPct(lap.throttleBrakeOverlapPct),
      "- Steering smoothness: " + reportFormatNumber(lap.steeringSmoothness, 4),
      ""
    );

    if (lap.brakingZones?.length) {
      lines.push("Braking zones:");
      for (const zone of lap.brakingZones.slice(0, 6)) {
        lines.push(
          "- " + zone.zoneId +
            " " + reportFormatNumber(zone.startLapDistanceM, 1, " m") +
            "-" + reportFormatNumber(zone.endLapDistanceM, 1, " m") +
            ": entry " + reportFormatNumber(zone.entrySpeedKph, 0, " kph") +
            ", min " + reportFormatNumber(zone.minSpeedKph, 0, " kph") +
            ", exit " + reportFormatNumber(zone.exitSpeedKph, 0, " kph") +
            ", peak brake " + reportFormatPct(zone.peakBrakePct) +
            ", trail brake " + reportFormatPct(zone.trailBrakePctOfZone) +
            ", coast after brake " + reportFormatNumber(zone.coastingAfterBrakeSec, 2, " sec") +
            (zone.comparisonToBestLap?.deltaExitSpeedKph != null ? ", exit vs best " + reportFormatNumber(zone.comparisonToBestLap.deltaExitSpeedKph, 1, " kph") : "") +
            (zone.issueTags?.length ? ", flags: " + zone.issueTags.join(", ") : "")
        );
      }
      lines.push("");
    }

    if (lap.corneringZones?.length) {
      lines.push("Cornering zones:");
      for (const zone of lap.corneringZones.slice(0, 6)) {
        lines.push(
          "- " + zone.zoneId +
            " " + reportFormatNumber(zone.startLapDistanceM, 1, " m") +
            "-" + reportFormatNumber(zone.endLapDistanceM, 1, " m") +
            ": apex " + reportFormatNumber(zone.apexSpeedKph, 0, " kph") +
            ", exit " + reportFormatNumber(zone.exitSpeedKph, 0, " kph") +
            ", throttle at apex " + reportFormatPct(zone.throttleAtApexPct) +
            ", coasting " + reportFormatPct(zone.coastingPct) +
            ", peak steering " + reportFormatNumber(zone.peakAbsSteering, 3) +
            (zone.comparisonToBestLap?.deltaExitSpeedKph != null ? ", exit vs best " + reportFormatNumber(zone.comparisonToBestLap.deltaExitSpeedKph, 1, " kph") : "") +
            (zone.issueTags?.length ? ", flags: " + zone.issueTags.join(", ") : "")
        );
      }
      lines.push("");
    }
    if (lap.apexCornerAnalysis?.status === "ready" && lap.apexCornerAnalysis.corners?.length) {
      lines.push("Apex corner analysis:");
      for (const corner of lap.apexCornerAnalysis.corners.slice(0, 8)) {
        lines.push(
          "- " + corner.cornerId +
            " apex " + reportFormatNumber(corner.apexDistanceM, 1, " m") +
            ": min " + reportFormatNumber(corner.minCornerSpeedKph, 1, " kph") +
            " (" + reportFormatNumber(corner.deltas?.minSpeedDeltaKph, 1, " kph vs ref") + ")" +
            ", brake " + reportFormatNumber(corner.brakeDistanceBeforeApexM, 1, " m before apex") +
            (corner.deltas?.brakeDistanceDeltaM != null ? " (" + reportFormatNumber(corner.deltas.brakeDistanceDeltaM, 1, " m vs ref") + ")" : "") +
            ", exit " + reportFormatNumber(corner.exitSpeedKph, 1, " kph") +
            (corner.exitMeasurementConfidence === "high" && corner.deltas?.exitSpeedDeltaKph != null ? " (" + reportFormatNumber(corner.deltas.exitSpeedDeltaKph, 1, " kph vs ref") + ")" : " (exit confidence low)") +
            ", full throttle " + reportFormatNumber(corner.exitDistanceFromApexM, 1, " m after apex") +
            (corner.deltas?.exitDistanceDeltaM != null ? " (" + reportFormatNumber(corner.deltas.exitDistanceDeltaM, 1, " m vs ref") + ")" : "") +
            (corner.issueTags?.length ? ", flags: " + corner.issueTags.join(", ") : "")
        );
      }
      lines.push("");
    }
  }

  lines.push(
    "## 10. Requested AI Output",
    "",
    "1. Session overview in 3 short bullets.",
    "2. Top 3 coaching tips, each with braking/corner evidence.",
    "3. One lap-specific note comparing a weak lap to the best actual lap and theoretical best lap.",
    "4. One braking drill and one corner-exit drill for the next session.",
    "5. Confidence score based on data quality.",
    ""
  );

  return lines.join("\n");
}

function buildPostSessionReportDocument(sessionId, sessionData, samples, lapDocs, cornerDocs, options = {}) {
  const reportPhase = safeString(options.phase, null) || (isClosedSession(sessionData) ? "final" : "live");
  const reportTrigger = safeString(options.trigger, null) || (reportPhase === "final" ? "session_end" : "lap");
  const triggerLapNumber = parseInteger(options.triggerLapNumber, null);
  const samplesByLap = new Map();
  for (const sample of samples) {
    if (sample.lapNumber === null) continue;
    if (!samplesByLap.has(sample.lapNumber)) samplesByLap.set(sample.lapNumber, []);
    samplesByLap.get(sample.lapNumber).push(sample);
  }

  const lapByNumber = new Map();
  for (const lap of lapDocs) {
    const lapNumber = parseInteger(lap.lapNumber, null);
    if (lapNumber !== null) lapByNumber.set(lapNumber, lap);
  }

  const cornersByLap = new Map();
  for (const corner of cornerDocs) {
    const lapNumber = parseInteger(corner.startLapNumber ?? corner.endLapNumber, null);
    if (lapNumber === null) continue;
    if (!cornersByLap.has(lapNumber)) cornersByLap.set(lapNumber, []);
    cornersByLap.get(lapNumber).push(corner);
  }

  const lapNumbers = capReportLapNumbers([...new Set([...samplesByLap.keys(), ...lapByNumber.keys()])].sort((a, b) => a - b), options);
  let lapSummaries = lapNumbers.map((lapNumber) =>
    summarizeReportLap(
      lapNumber,
      samplesByLap.get(lapNumber) || [],
      lapByNumber.get(lapNumber) || null,
      cornersByLap.get(lapNumber) || []
    )
  );

  const speeds = samples.map((sample) => sample.speedKph).filter((value) => value !== null);
  const validTimedLaps = lapSummaries.filter((lap) => lap.valid !== false && lap.lapTimeMs !== null);
  const bestLap = validTimedLaps.length
    ? validTimedLaps.reduce((best, lap) => (lap.lapTimeMs < best.lapTimeMs ? lap : best), validTimedLaps[0])
    : null;
  const worstLap = validTimedLaps.length
    ? validTimedLaps.reduce((worst, lap) => (lap.lapTimeMs > worst.lapTimeMs ? lap : worst), validTimedLaps[0])
    : null;

  const apexCornerAnalysis = buildApexCornerAnalysisForLaps(lapSummaries, samplesByLap, bestLap);
  lapSummaries = apexCornerAnalysis.lapSummaries;
  const apexCornerAnalysisSummary = apexCornerAnalysis.summary;
  const apexCornerReference = apexCornerAnalysis.reference;

  if (bestLap) {
    lapSummaries = addZoneComparisons(lapSummaries, bestLap.lapNumber);
  }

  const theoreticalBestLap = buildTheoreticalBestLap(lapDocs, bestLap);
  const dataQuality = buildDataQuality(samples, lapDocs, cornerDocs);
  const precisionFindings = buildPrecisionFindings(lapSummaries, bestLap);
  const topCoachSignals = buildPostSessionCoachSignals(lapSummaries, precisionFindings, bestLap);
  const lapComparisonTable = buildLapComparisonTable(lapSummaries, bestLap, theoreticalBestLap);
  const status = samples.length || lapDocs.length ? "ready" : "empty";

  const report = stripUndefinedDeep({
    schema: POST_SESSION_REPORT_SCHEMA,
    status,
    reportPhase,
    reportTrigger,
    triggerLapNumber,
    generatedAtIso: new Date().toISOString(),
    dataQuality,
    sessionSnapshot: {
      sessionId,
      userId: safeString(sessionData.userId, null),
      username: safeString(sessionData.username, null),
      trackId: parseInteger(sessionData.trackId, null),
      trackName: safeString(sessionData.trackName, null),
      sessionType: parseInteger(sessionData.sessionType, null),
      startedAt: reportIso(sessionData.startedAt),
      endedAt: reportIso(sessionData.endedAt),
      reportPhase,
      reportTrigger,
      triggerLapNumber,
      sampleCount: samples.rawSampleCount ?? samples.length,
      analysedSampleCount: samples.length,
      lapCount: lapNumbers.length,
      cornerCount: cornerDocs.length,
      avgSpeedKph: reportRound(reportAvg(speeds), 1),
      topSpeedKph: speeds.length ? reportRound(Math.max(...speeds), 1) : null,
    },
    bestLap: bestLap
      ? {
          lapNumber: bestLap.lapNumber,
          lapTimeMs: bestLap.lapTimeMs,
          lapTime: bestLap.lapTime,
        }
      : null,
    worstLap: worstLap
      ? {
          lapNumber: worstLap.lapNumber,
          lapTimeMs: worstLap.lapTimeMs,
          lapTime: worstLap.lapTime,
          gapToBestMs: bestLap ? worstLap.lapTimeMs - bestLap.lapTimeMs : null,
        }
      : null,
    theoreticalBestLap,
    apexCornerAnalysisSummary,
    apexCornerReference,
    lapComparisonTable,
    lapSummaries,
    precisionFindings,
    topCoachSignals,
  });

  report.aiReadableMarkdown = limitReportText(
    renderPostSessionMarkdown({
      ...report,
      lapSummaries: report.lapSummaries.slice(0, MAX_MARKDOWN_LAPS),
      lapComparisonTable: report.lapComparisonTable.slice(0, MAX_MARKDOWN_LAPS),
      precisionFindings: report.precisionFindings.slice(0, MAX_REPORT_FINDINGS),
      topCoachSignals: report.topCoachSignals.slice(0, MAX_REPORT_SIGNALS),
    })
  );
  return report;
}

function limitReportText(text, maxChars = MAX_AI_MARKDOWN_CHARS) {
  const value = String(text || "");
  if (value.length <= maxChars) return value;
  return (
    value.slice(0, maxChars) +
    "\n\n[Report truncated for storage safety. Full structured lap detail is stored in the reports/postSession/laps subcollection.]"
  );
}

function compactReportZone(zone) {
  return stripUndefinedDeep({
    zoneId: zone.zoneId,
    startLapDistanceM: zone.startLapDistanceM,
    endLapDistanceM: zone.endLapDistanceM,
    entrySpeedKph: zone.entrySpeedKph,
    minSpeedKph: zone.minSpeedKph,
    apexSpeedKph: zone.apexSpeedKph,
    exitSpeedKph: zone.exitSpeedKph,
    peakBrakePct: zone.peakBrakePct,
    coastingAfterBrakeSec: zone.coastingAfterBrakeSec,
    coastingPct: zone.coastingPct,
    throttlePickupDelaySec: zone.throttlePickupDelaySec,
    issueTags: Array.isArray(zone.issueTags) ? zone.issueTags.slice(0, 4) : [],
    comparisonToBestLap: zone.comparisonToBestLap || null,
  });
}

function compactLapForMainReport(lap) {
  const importantBrakingZones = (lap.brakingZones || [])
    .filter((zone) => (zone.issueTags || []).length || zone.comparisonToBestLap)
    .slice(0, 3)
    .map(compactReportZone);

  const importantCorneringZones = (lap.corneringZones || [])
    .filter((zone) => (zone.issueTags || []).length || zone.comparisonToBestLap)
    .slice(0, 3)
    .map(compactReportZone);

  const importantApexCorners = (lap.apexCornerAnalysis?.corners || [])
    .filter((corner) => (corner.issueTags || []).length || scoreApexCornerComparison(corner) > 0)
    .slice(0, 4)
    .map((corner) => stripUndefinedDeep({
      cornerId: corner.cornerId,
      apexDistanceM: corner.apexDistanceM,
      minCornerSpeedKph: corner.minCornerSpeedKph,
      brakeDistanceBeforeApexM: corner.brakeDistanceBeforeApexM,
      exitSpeedKph: corner.exitSpeedKph,
      exitDistanceFromApexM: corner.exitDistanceFromApexM,
      exitMeasurementConfidence: corner.exitMeasurementConfidence,
      deltas: corner.deltas,
      issueTags: corner.issueTags,
    }));

  return stripUndefinedDeep({
    lapNumber: lap.lapNumber,
    sampleCount: lap.sampleCount,
    lapTimeMs: lap.lapTimeMs,
    lapTime: lap.lapTime,
    sector1Ms: lap.sector1Ms,
    sector2Ms: lap.sector2Ms,
    sector3Ms: lap.sector3Ms,
    valid: lap.valid,
    avgSpeedKph: lap.avgSpeedKph,
    maxSpeedKph: lap.maxSpeedKph,
    fullThrottlePct: lap.fullThrottlePct,
    heavyBrakePct: lap.heavyBrakePct,
    coastingPct: lap.coastingPct,
    throttleBrakeOverlapPct: lap.throttleBrakeOverlapPct,
    avgAbsSteering: lap.avgAbsSteering,
    steeringSmoothness: lap.steeringSmoothness,
    brakingZoneCount: lap.brakingZoneCount,
    corneringZoneCount: lap.corneringZoneCount,
    importantBrakingZones,
    importantCorneringZones,
    apexCornerSummary: lap.apexCornerAnalysis?.summary || null,
    importantApexCorners,
  });
}

function buildCompactPostSessionReport(report) {
  return stripUndefinedDeep({
    ...report,
    lapSummaries: (report.lapSummaries || []).map(compactLapForMainReport),
    precisionFindings: (report.precisionFindings || []).slice(0, MAX_REPORT_FINDINGS),
    topCoachSignals: (report.topCoachSignals || []).slice(0, MAX_REPORT_SIGNALS),
    aiReadableMarkdown: limitReportText(report.aiReadableMarkdown),
    storageNote:
      "Main report is compact to avoid Render/Firestore memory limits. Detailed braking and cornering evidence is stored in reports/postSession/laps.",
  });
}

async function commitReportLapDetails(reportRef, lapSummaries, schema, sessionId) {
  let batch = db.batch();
  let writes = 0;
  let totalWrites = 0;

  for (const lap of (lapSummaries || []).slice(0, MAX_REPORT_LAP_DETAIL_DOCS)) {
    const lapRef = reportRef.collection("laps").doc("lap_" + lap.lapNumber);

    batch.set(lapRef, {
      ...stripUndefinedDeep(lap),
      schema,
      sessionId,
      updatedAt: FieldValue.serverTimestamp(),
    });

    writes += 1;
    totalWrites += 1;

    if (writes >= 400) {
      await batch.commit();
      batch = db.batch();
      writes = 0;
    }
  }

  if (writes > 0) {
    await batch.commit();
  }

  return totalWrites;
}

async function buildAndSavePostSessionReport(sessionRef, options = {}) {
  const sessionSnap = await sessionRef.get();
  if (!sessionSnap.exists) {
    const err = new Error("session not found");
    err.status = 404;
    throw err;
  }

  const sessionData = serializeDoc(sessionSnap);
  const lapDocs = await collectSessionLapDocs(sessionRef);
  const targetLapNumbers = chooseReportLapNumbers(lapDocs, options);
  const phase = safeString(options.phase, null) || (isClosedSession(sessionData) ? "final" : "live");
  const maxAnalyzedLaps = phase === "live" ? MAX_LIVE_REPORT_ANALYZED_LAPS : MAX_FINAL_REPORT_ANALYZED_LAPS;
  const samples = await collectSessionTelemetrySamples(sessionRef, {
    ...options,
    targetLapNumbers,
    maxAnalyzedLaps,
  });
  const cornerDocs = await collectSessionCornerDocs(sessionRef, targetLapNumbers);
  const report = buildPostSessionReportDocument(
    sessionRef.id,
    sessionData,
    samples,
    lapDocs,
    cornerDocs,
    options
  );
  const reportPath = "sessions/" + sessionRef.id + "/reports/postSession";
  const reportRef = sessionRef.collection("reports").doc("postSession");

  const mainReport = buildCompactPostSessionReport(report);

  await reportRef.set(
    stripUndefinedDeep({
      ...mainReport,
      reportPath,
      generatedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    }),
    { merge: false }
  );

  const lapDetailDocCount = await commitReportLapDetails(
    reportRef,
    report.lapSummaries || [],
    report.schema,
    sessionRef.id
  );

  await sessionRef.set(
    stripUndefinedDeep({
      postSessionReportStatus: report.status,
      postSessionReportPath: reportPath,
      postSessionReportUpdatedAt: FieldValue.serverTimestamp(),
      postSessionReportSummary: {
        schema: report.schema,
        reportPhase: report.reportPhase,
        reportTrigger: report.reportTrigger,
        triggerLapNumber: report.triggerLapNumber,
        dataConfidence: report.dataQuality.confidence,
        sampleCount: report.sessionSnapshot.sampleCount,
        lapCount: report.sessionSnapshot.lapCount,
        coachSignalCount: report.topCoachSignals.length,
        precisionFindingCount: report.precisionFindings.length,
        lapDetailDocCount,
        brakingFindingCount: report.precisionFindings.filter((finding) => finding.type === "braking").length,
        cornerFindingCount: report.precisionFindings.filter((finding) => finding.type === "cornering").length,
        bestLap: report.bestLap,
        theoreticalBestLap: report.theoreticalBestLap,
        apexCornerAnalysis: report.apexCornerAnalysisSummary,
        worstLap: report.worstLap,
      },
    }),
    { merge: true }
  );

  return {
    status: report.status,
    reportPath,
    reportPhase: report.reportPhase,
    reportTrigger: report.reportTrigger,
    triggerLapNumber: report.triggerLapNumber,
    dataConfidence: report.dataQuality.confidence,
    sampleCount: report.sessionSnapshot.sampleCount,
    lapCount: report.sessionSnapshot.lapCount,
    coachSignalCount: report.topCoachSignals.length,
    precisionFindingCount: report.precisionFindings.length,
    lapDetailDocCount,
    brakingFindingCount: report.precisionFindings.filter((finding) => finding.type === "braking").length,
    cornerFindingCount: report.precisionFindings.filter((finding) => finding.type === "cornering").length,
    bestLap: report.bestLap,
    theoreticalBestLap: report.theoreticalBestLap,
    apexCornerAnalysis: report.apexCornerAnalysisSummary,
    worstLap: report.worstLap,
  };
}
const livePostSessionReportJobs = new Map();

function mergeLiveReportOptions(previous = {}, next = {}) {
  const prevLap = parseInteger(previous.triggerLapNumber, null);
  const nextLap = parseInteger(next.triggerLapNumber, null);

  return {
    ...previous,
    ...next,
    triggerLapNumber:
      nextLap !== null && (prevLap === null || nextLap >= prevLap) ? nextLap : prevLap,
  };
}

async function runLivePostSessionReportQueue(sessionRef, state) {
  state.running = true;
  state.timer = null;
  state.promise = (async () => {
    try {
      while (state.pending) {
        const options = state.pending;
        state.pending = null;

        await sessionRef.set(
          stripUndefinedDeep({
            postSessionReportStatus: "updating",
            postSessionReportPhase: options.phase || "live",
            postSessionReportTrigger: options.trigger || "lap",
            postSessionReportTriggerLapNumber: parseInteger(options.triggerLapNumber, null),
            postSessionReportQueuedAt: FieldValue.serverTimestamp(),
          }),
          { merge: true }
        );

        try {
          await buildAndSavePostSessionReport(sessionRef, options);
        } catch (err) {
          console.error("Live post-session report update error:", err);
          await sessionRef.set(
            stripUndefinedDeep({
              postSessionReportStatus: "failed",
              postSessionReportError: err.message || "failed to update live report",
              postSessionReportUpdatedAt: FieldValue.serverTimestamp(),
            }),
            { merge: true }
          );
        }
      }
    } finally {
      state.running = false;
      if (!state.pending) {
        livePostSessionReportJobs.delete(sessionRef.id);
      } else {
        scheduleLivePostSessionReportBuild(sessionRef, state, POST_SESSION_LIVE_REPORT_DEBOUNCE_MS);
      }
    }
  })();

  await state.promise;
}

function scheduleLivePostSessionReportBuild(sessionRef, state, delayMs) {
  if (state.running) return;
  if (state.timer) clearTimeout(state.timer);

  state.timer = setTimeout(() => {
    runLivePostSessionReportQueue(sessionRef, state).catch((err) => {
      console.error("Live post-session report queue error:", err);
    });
  }, Math.max(0, delayMs));
}

function queueLivePostSessionReportBuild(sessionRef, options = {}) {
  if (!POST_SESSION_LIVE_REPORT_ENABLED) return false;

  const sessionId = sessionRef.id;
  const state =
    livePostSessionReportJobs.get(sessionId) || {
      running: false,
      pending: null,
      promise: null,
      timer: null,
    };

  state.pending = mergeLiveReportOptions(state.pending || {}, {
    phase: "live",
    trigger: "lap",
    ...options,
  });

  livePostSessionReportJobs.set(sessionId, state);
  scheduleLivePostSessionReportBuild(sessionRef, state, POST_SESSION_LIVE_REPORT_DEBOUNCE_MS);
  return true;
}

function getLivePostSessionReportPromise(sessionId) {
  const state = livePostSessionReportJobs.get(sessionId);
  return state?.promise || null;
}

// POST_SESSION_AI_REPORT_END

app.post("/sessions/:id/end", async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const sessionData = sessionSnap.data() || {};
    const endMetadata = buildEndMetadata(req);
    const requestedEndedAt = endMetadata.endedAt || null;
    const currentEndedAt = asDate(sessionData.endedAt);
    const rebuildReport =
      parseBoolean(req.query.rebuildReport, false) ||
      parseBoolean(req.body?.rebuildReport, false);

    if (
      isClosedSession(sessionData) &&
      !rebuildReport &&
      requestedEndedAt &&
      (!currentEndedAt || requestedEndedAt.getTime() < currentEndedAt.getTime())
    ) {
      await sessionRef.set(
        stripUndefinedDeep({
          endedAt: requestedEndedAt,
          endedAtCorrectedAt: FieldValue.serverTimestamp(),
          endedAtServerReceivedAt: FieldValue.serverTimestamp(),
          endedAtSource: endMetadata.endedAtSource,
          endReason: endMetadata.endReason,
          endPacketType: endMetadata.endPacketType,
          listenerClosedAt: endMetadata.listenerClosedAt,
        }),
        { merge: true }
      );
    }

    if (isClosedSession(sessionData) && !rebuildReport) {
      const reportRef = sessionRef.collection("reports").doc("postSession");
      const reportSnap = await reportRef.get();
      const existingReport = reportSnap.exists ? serializeDoc(reportSnap) : null;

      return res.json({
        ...serializeDoc(sessionSnap),
        postSessionReport: existingReport
          ? {
              status: "ready",
              alreadyEnded: true,
              reportPath: "sessions/" + sessionId + "/reports/postSession",
              reportPhase: existingReport.reportPhase || "final",
              reportTrigger: existingReport.reportTrigger || "session_end",
              sampleCount:
                existingReport.sessionSnapshot?.sampleCount ??
                existingReport.summary?.sampleCount ??
                null,
              lapCount:
                existingReport.lapSummaries?.length ??
                existingReport.summary?.lapCount ??
                null,
              coachSignalCount:
                existingReport.topCoachSignals?.length ??
                existingReport.summary?.coachSignalCount ??
                null,
              lapDetailDocCount: existingReport.summary?.lapDetailDocCount ?? null,
            }
          : {
              status: sessionData.postSessionReportStatus || "already-ended",
              alreadyEnded: true,
              message: "Session was already ended; report was not rebuilt.",
            },
      });
    }

    const liveReportPromise = getLivePostSessionReportPromise(sessionId);
    if (liveReportPromise) {
      await liveReportPromise.catch(() => null);
    }

    await sessionRef.set(
      stripUndefinedDeep({
        endedAt: requestedEndedAt || FieldValue.serverTimestamp(),
        endedAtServerReceivedAt: FieldValue.serverTimestamp(),
        endedAtSource: endMetadata.endedAtSource,
        endReason: endMetadata.endReason,
        endPacketType: endMetadata.endPacketType,
        listenerClosedAt: endMetadata.listenerClosedAt,
        postSessionReportStatus: "building",
        postSessionReportPhase: "final",
        postSessionReportTrigger: "session_end",
      }),
      { merge: true }
    );

    let postSessionReport = null;

    try {
      if (!POST_SESSION_FINAL_REPORT_ENABLED) {
        postSessionReport = {
          status: "skipped",
          reportPhase: "final",
          reportTrigger: "session_end",
          reason: "final post-session reports disabled",
        };
        await sessionRef.set(
          {
            postSessionReportStatus: "skipped",
            postSessionReportPhase: "final",
            postSessionReportTrigger: "session_end",
            postSessionReportUpdatedAt: FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
      } else {
        postSessionReport = await buildAndSavePostSessionReport(sessionRef, {
          phase: "final",
          trigger: "session_end",
        });
      }
    } catch (reportErr) {
      console.error("Post-session report generation error:", reportErr);
      postSessionReport = {
        status: "failed",
        error: reportErr.message || "failed to build post-session report",
      };

      await sessionRef.set(
        {
          postSessionReportStatus: "failed",
          postSessionReportError: postSessionReport.error,
          postSessionReportUpdatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    }

    const updated = await sessionRef.get();
    res.json({
      ...serializeDoc(updated),
      postSessionReport,
    });
  } catch (err) {
    console.error("POST /sessions/:id/end error:", err);
    res.status(500).json({ error: "failed to end session" });
  }
});

app.get("/sessions/:id/reports/post-session", authenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }
    if (!requireReadableSession(req, res, sessionSnap.data() || {})) return;

    const reportSnap = await sessionRef.collection("reports").doc("postSession").get();
    if (!reportSnap.exists) {
      return res.status(404).json({ error: "post-session report not found" });
    }

    res.json(serializeDoc(reportSnap));
  } catch (err) {
    console.error("GET /sessions/:id/reports/post-session error:", err);
    res.status(500).json({ error: "failed to fetch post-session report" });
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

    if (isClosedSession(sessionSnap.data() || {})) {
      return res.json({
        success: true,
        ignored: true,
        reason: "session ended",
        lapNumber,
      });
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

    const shouldUpdateReport =
      parseBoolean(req.query.updateReport, true) !== false &&
      parseBoolean(req.body.updateReport, true) !== false;

    let reportQueued = false;
    if (shouldUpdateReport) {
      reportQueued = queueLivePostSessionReportBuild(sessionRef, {
        triggerLapNumber: lapNumber,
      });
    }

    res.status(201).json({
      success: true,
      lapNumber,
      postSessionReport: reportQueued
        ? {
            status: "queued",
            reportPhase: "live",
            reportTrigger: "lap",
            triggerLapNumber: lapNumber,
            reportPath: "sessions/" + sessionId + "/reports/postSession",
          }
        : {
            status: "skipped",
            reportPhase: "live",
            reportTrigger: "lap",
            triggerLapNumber: lapNumber,
            reason: shouldUpdateReport ? "live reports disabled" : "updateReport disabled",
          },
    });
  } catch (err) {
    console.error("POST /sessions/:id/laps error:", err);
    res.status(500).json({ error: "failed to save lap" });
  }
});


// LAP_PERFORMANCE_ANALYSIS_PATCH
function lapPerfRound(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const scale = 10 ** digits;
  return Math.round(n * scale) / scale;
}

function lapPerfAvg(values, digits = 2) {
  const nums = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  if (!nums.length) return null;
  return lapPerfRound(nums.reduce((sum, value) => sum + value, 0) / nums.length, digits);
}

function lapPerfMin(values, digits = 2) {
  const nums = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  return nums.length ? lapPerfRound(Math.min(...nums), digits) : null;
}

function lapPerfMax(values, digits = 2) {
  const nums = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  return nums.length ? lapPerfRound(Math.max(...nums), digits) : null;
}

function lapPerfControlFraction(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1.5 ? n / 100 : n;
}

function lapPerfControlPct(value) {
  const fraction = lapPerfControlFraction(value);
  return fraction === null ? null : lapPerfRound(Math.max(0, Math.min(1, fraction)) * 100, 1);
}

function lapPerfSortSamples(samples) {
  return [...samples].sort((a, b) => {
    const ad = finiteNumberOrNull(a.lapDistance);
    const bd = finiteNumberOrNull(b.lapDistance);
    if (ad !== null && bd !== null && ad !== bd) return ad - bd;

    const ai = parseInteger(a.sampleIndex, 0) || 0;
    const bi = parseInteger(b.sampleIndex, 0) || 0;
    return ai - bi;
  });
}

function lapPerfDistanceSpan(samples, key = "lapDistance") {
  const distances = samples
    .map((sample) => finiteNumberOrNull(sample[key]))
    .filter((value) => value !== null);

  if (!distances.length) return null;
  return lapPerfRound(Math.max(...distances) - Math.min(...distances), 1);
}

function lapPerfBuildBrakingStats(samples) {
  const runs = [];
  let current = [];

  for (const sample of samples) {
    const brake = lapPerfControlFraction(sample.brake) || 0;
    if (brake >= 0.08) {
      current.push(sample);
    } else {
      if (current.length >= 2) runs.push(current);
      current = [];
    }
  }

  if (current.length >= 2) runs.push(current);

  const computedDistances = runs
    .map((run) => lapPerfDistanceSpan(run, "lapDistance"))
    .filter((value) => value !== null);
  const reportedDistances = samples
    .map((sample) => finiteNumberOrNull(sample.brakingDistance))
    .filter((value) => value !== null && value >= 0);

  return stripUndefinedDeep({
    zoneCount: runs.length,
    longestDistanceM: lapPerfMax([...computedDistances, ...reportedDistances], 1),
    maxBrakePct: lapPerfMax(samples.map((sample) => lapPerfControlPct(sample.brake)), 1),
    averageBrakePct: lapPerfAvg(samples.map((sample) => lapPerfControlPct(sample.brake)), 1),
  });
}

function lapPerfBuildCorneringStats(samples, corners) {
  const cornerMinSpeeds = (corners || [])
    .map((corner) => finiteNumberOrNull(corner.minSpeedKph))
    .filter((value) => value !== null);
  const explicitCornerSpeeds = samples
    .map((sample) => finiteNumberOrNull(sample.corneringSpeed))
    .filter((value) => value !== null);
  const steeringCornerSpeeds = samples
    .filter((sample) => Math.abs(Number(sample.steering ?? 0)) >= 0.25)
    .map((sample) => finiteNumberOrNull(sample.speedKph))
    .filter((value) => value !== null);
  const speeds = cornerMinSpeeds.length
    ? cornerMinSpeeds
    : explicitCornerSpeeds.length
      ? explicitCornerSpeeds
      : steeringCornerSpeeds;

  return stripUndefinedDeep({
    cornerCount: cornerMinSpeeds.length || null,
    averageSpeedKph: lapPerfAvg(speeds, 1),
    minSpeedKph: lapPerfMin(speeds, 1),
    maxSpeedKph: lapPerfMax(speeds, 1),
  });
}

function lapPerfBuildTraces(samples, maxSamples) {
  const sorted = lapPerfSortSamples(samples);
  const sampled = downsampleReportSamples(sorted, maxSamples);
  const firstDistance = sampled.find((sample) => finiteNumberOrNull(sample.lapDistance) !== null)?.lapDistance ?? null;

  return sampled.map((sample, index) => {
    const lapDistance = finiteNumberOrNull(sample.lapDistance);
    const distanceM =
      lapDistance !== null && firstDistance !== null
        ? Math.max(0, lapDistance - firstDistance)
        : lapDistance;

    return stripUndefinedDeep({
      index,
      sampleIndex: parseInteger(sample.sampleIndex, null),
      timestamp: sample.timestamp || null,
      lapDistance: lapPerfRound(lapDistance, 2),
      distanceM: lapPerfRound(distanceM, 2),
      worldX: lapPerfRound(sample.worldX, 3),
      worldY: lapPerfRound(sample.worldY, 3),
      worldZ: lapPerfRound(sample.worldZ, 3),
      steering: lapPerfRound(sample.steering, 4),
      pitStatus: parseInteger(sample.pitStatus, null),
      speedKph: lapPerfRound(sample.speedKph, 1),
      throttlePct: lapPerfControlPct(sample.throttle),
      brakePct: lapPerfControlPct(sample.brake),
      rpm: parseInteger(sample.rpm, null),
      gear: parseInteger(sample.gear, null),
      deltaToPB: lapPerfRound(sample.deltaToPB, 3),
      corneringSpeedKph: lapPerfRound(sample.corneringSpeed, 1),
      brakingDistanceM: lapPerfRound(sample.brakingDistance, 1),
      drs: parseBoolean(sample.drs, false) === true,
      sector: parseInteger(sample.currentSector, null),
    });
  });
}

// LAP_PERFORMANCE_SPEED_PATCH
const lapPerfPersonalBestCache = new Map();
const LAP_PERF_PB_CACHE_MS = 5 * 60 * 1000;

async function lapPerfFindPersonalBest(userId, trackKey) {
  if (!userId || !trackKey) return null;

  const cacheKey = String(userId) + "|" + String(trackKey);
  const cached = lapPerfPersonalBestCache.get(cacheKey);
  if (cached && Date.now() - cached.savedAt < LAP_PERF_PB_CACHE_MS) {
    return cached.value;
  }

  const sessionsSnap = await db
    .collection("sessions")
    .where("userId", "==", userId)
    .limit(300)
    .get();
  const matchingSessions = sessionsSnap.docs.filter((sessionDoc) => {
    const sessionData = serializeDoc(sessionDoc);
    const candidateTrackKey =
      safeString(sessionData.trackKey, null) ||
      trackKeyFrom(sessionData.trackId, sessionData.trackName);
    return candidateTrackKey === trackKey;
  });
  let best = null;
  const concurrency = 12;

  for (let offset = 0; offset < matchingSessions.length; offset += concurrency) {
    const sessionBatch = matchingSessions.slice(offset, offset + concurrency);
    const lapSnapshots = await Promise.all(
      sessionBatch.map((sessionDoc) => sessionDoc.ref.collection("laps").get())
    );

    for (let sessionIndex = 0; sessionIndex < sessionBatch.length; sessionIndex += 1) {
      const sessionDoc = sessionBatch[sessionIndex];
      const lapsSnap = lapSnapshots[sessionIndex];

      for (const lapDoc of lapsSnap.docs) {
        const lapData = serializeDoc(lapDoc);
        if (!isRealValidLeaderboardLap(lapData)) continue;

        const lapTimeMs = parseInteger(lapData.lapTimeMs, null);
        if (lapTimeMs === null) continue;

        if (!best || lapTimeMs < best.lapTimeMs) {
          best = stripUndefinedDeep({
            sessionId: sessionDoc.id,
            lapId: lapDoc.id,
            lapNumber: parseInteger(lapData.lapNumber, null),
            lapTimeMs,
            lapTime: formatLapTime(lapTimeMs),
            sector1Ms: parseInteger(lapData.sector1Ms, null),
            sector2Ms: parseInteger(lapData.sector2Ms, null),
            sector3Ms: parseInteger(lapData.sector3Ms, null),
            recordedAt: lapData.recordedAt || null,
          });
        }
      }
    }
  }

  lapPerfPersonalBestCache.set(cacheKey, {
    savedAt: Date.now(),
    value: best,
  });
  return best;
}

app.get("/sessions/:id/laps/:lapId/performance", optionalAuthenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const requestedLapId = safeString(req.params.lapId);
    const maxSamples = Math.max(60, Math.min(parseInteger(req.query.maxSamples, 420) || 420, 900));

    if (!sessionId || !requestedLapId) {
      return res.status(400).json({ error: "sessionId and lapId are required" });
    }

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }
    let lapRef = sessionRef.collection("laps").doc(requestedLapId);
    let lapSnap = await lapRef.get();
    const requestedLapNumber = parseInteger(requestedLapId, null);

    if (!lapSnap.exists && requestedLapNumber !== null) {
      lapRef = sessionRef.collection("laps").doc("lap_" + requestedLapNumber);
      lapSnap = await lapRef.get();
    }

    if (!lapSnap.exists) {
      return res.status(404).json({ error: "lap not found" });
    }

    const sessionData = serializeDoc(sessionSnap);
    const lapData = serializeDoc(lapSnap);

    if (!canReadSession(req, sessionData) && !isRealValidLeaderboardLap(lapData)) {
      return res.status(403).json({ error: "only valid leaderboard laps are public" });
    }

    const lapNumber = parseInteger(lapData.lapNumber, null);

    if (lapNumber === null) {
      return res.status(400).json({ error: "lap is missing lapNumber" });
    }

    const trackKey =
      safeString(sessionData.trackKey, null) ||
      trackKeyFrom(sessionData.trackId, sessionData.trackName);
    const [personalBest, samples, corners, reportLapSnap] = await Promise.all([
      lapPerfFindPersonalBest(sessionData.userId, trackKey),
      collectSessionTelemetrySamples(sessionRef, {
        targetLapNumbers: [lapNumber],
        maxSamplesPerLap: maxSamples,
        maxTotalSamples: maxSamples,
        maxAnalyzedLaps: 1,
        chunkPageSize: 20,
      }),
      collectSessionCornerDocs(sessionRef, [lapNumber]),
      sessionRef
        .collection("reports")
        .doc("postSession")
        .collection("laps")
        .doc("lap_" + lapNumber)
        .get()
        .catch(() => null),
    ]);
    const reportLapData = reportLapSnap?.exists ? serializeDoc(reportLapSnap) : null;
    const lapSamples = lapPerfSortSamples(
      samples.filter((sample) => parseInteger(sample.lapNumber, null) === lapNumber)
    );
    const traces = lapPerfBuildTraces(lapSamples, maxSamples);
    const lapTimeMs = parseInteger(lapData.lapTimeMs, null);

    const toPersonalBestMs =
      personalBest && lapTimeMs !== null
        ? lapTimeMs - personalBest.lapTimeMs
        : null;
    const sectorDeltas = personalBest
      ? {
          sector1Ms:
            parseInteger(lapData.sector1Ms, null) !== null && personalBest.sector1Ms != null
              ? parseInteger(lapData.sector1Ms, null) - personalBest.sector1Ms
              : null,
          sector2Ms:
            parseInteger(lapData.sector2Ms, null) !== null && personalBest.sector2Ms != null
              ? parseInteger(lapData.sector2Ms, null) - personalBest.sector2Ms
              : null,
          sector3Ms:
            parseInteger(lapData.sector3Ms, null) !== null && personalBest.sector3Ms != null
              ? parseInteger(lapData.sector3Ms, null) - personalBest.sector3Ms
              : null,
        }
      : {};

    const drsActiveCount = lapSamples.filter((sample) => parseBoolean(sample.drs, false) === true).length;

    res.json(stripUndefinedDeep({
      session: {
        id: sessionId,
        userId: safeString(sessionData.userId, null),
        username: safeString(sessionData.username, null),
        trackName: safeString(sessionData.trackName, null),
        trackId: parseInteger(sessionData.trackId, null),
        trackKey,
        sessionType: parseInteger(sessionData.sessionType, null),
        startedAt: sessionData.startedAt || null,
        endedAt: sessionData.endedAt || null,
      },
      lap: {
        id: lapSnap.id,
        lapNumber,
        lapTimeMs,
        lapTime: formatLapTime(lapTimeMs),
        sector1Ms: parseInteger(lapData.sector1Ms, null),
        sector2Ms: parseInteger(lapData.sector2Ms, null),
        sector3Ms: parseInteger(lapData.sector3Ms, null),
        valid: lapData.valid === true,
        recordedAt: lapData.recordedAt || null,
      },
      personalBest,
      deltas: {
        toPersonalBestMs,
        sectors: sectorDeltas,
      },
      stats: {
        sampleCount: lapSamples.length,
        rawSampleCount: samples.rawSampleCount ?? lapSamples.length,
        speed: {
          minKph: lapPerfMin(lapSamples.map((sample) => sample.speedKph), 1),
          maxKph: lapPerfMax(lapSamples.map((sample) => sample.speedKph), 1),
          averageKph: lapPerfAvg(lapSamples.map((sample) => sample.speedKph), 1),
        },
        throttle: {
          averagePct: lapPerfAvg(lapSamples.map((sample) => lapPerfControlPct(sample.throttle)), 1),
          maxPct: lapPerfMax(lapSamples.map((sample) => lapPerfControlPct(sample.throttle)), 1),
        },
        brake: {
          averagePct: lapPerfAvg(lapSamples.map((sample) => lapPerfControlPct(sample.brake)), 1),
          maxPct: lapPerfMax(lapSamples.map((sample) => lapPerfControlPct(sample.brake)), 1),
        },
        rpm: {
          average: lapPerfAvg(lapSamples.map((sample) => sample.rpm), 0),
          max: lapPerfMax(lapSamples.map((sample) => sample.rpm), 0),
        },
        gear: {
          min: lapPerfMin(lapSamples.map((sample) => sample.gear), 0),
          max: lapPerfMax(lapSamples.map((sample) => sample.gear), 0),
        },
        cornering: lapPerfBuildCorneringStats(lapSamples, corners),
        braking: lapPerfBuildBrakingStats(lapSamples),
        drs: {
          used: drsActiveCount > 0,
          activeSampleCount: drsActiveCount,
          activePct: lapSamples.length ? lapPerfRound((drsActiveCount / lapSamples.length) * 100, 1) : 0,
        },
      },
      apexCornerAnalysis: reportLapData?.apexCornerAnalysis || null,
      traces,
      corners: corners.map((corner) => stripUndefinedDeep({
        id: corner.id,
        cornerIndex: parseInteger(corner.cornerIndex, null),
        startLapDistanceM: parseNumber(corner.startLapDistanceM, null),
        endLapDistanceM: parseNumber(corner.endLapDistanceM, null),
        minSpeedKph: parseNumber(corner.minSpeedKph, null),
        maxBrake: parseNumber(corner.maxBrake, null),
        maxThrottle: parseNumber(corner.maxThrottle, null),
      })),
      meta: {
        analysisType: "post_race_lap_performance",
        maxSamples,
      },
    }));
  } catch (err) {
    console.error("GET /sessions/:id/laps/:lapId/performance error:", err);
    res.status(500).json({ error: "failed to fetch lap performance" });
  }
});


app.get("/sessions/:id/laps", authenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }
    if (!requireReadableSession(req, res, sessionSnap.data() || {})) return;

    const lapsSnap = await sessionRef.collection("laps").get();
    const laps = lapsSnap.docs
      .map((doc) => serializeDoc(doc))
      .sort((a, b) => {
        const aLap = parseInteger(a.lapNumber, 0) || 0;
        const bLap = parseInteger(b.lapNumber, 0) || 0;
        return aLap - bLap;
      });

    res.json({ sessionId, laps });
  } catch (err) {
    console.error("GET /sessions/:id/laps error:", err);
    res.status(500).json({ error: "failed to fetch laps" });
  }
});

app.get("/sessions/:id/lap-trails", authenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const requestedMaxPointsPerLap = parseInteger(req.query.maxPointsPerLap, 400) || 400;
    const maxPointsPerLap = Math.max(50, Math.min(requestedMaxPointsPerLap, 600));
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }
    if (!requireReadableSession(req, res, sessionSnap.data() || {})) return;

    const points = await collectSessionMapPoints(sessionRef, {
      maxPoints: Math.max(500, maxPointsPerLap * MAX_LAP_TRAILS),
    });
    const lapTrails = buildLapTrailsFromMapPoints(points, maxPointsPerLap);

    res.json({
      sessionId,
      lapTrails,
      lapCount: lapTrails.length,
      sourcePointCount: points.rawPointCount ?? points.length,
    });
  } catch (err) {
    console.error("GET /sessions/:id/lap-trails error:", err);
    res.status(500).json({ error: "failed to fetch lap trails" });
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

    if (isClosedSession(sessionSnap.data() || {})) {
      return res.json({
        success: true,
        ignored: true,
        reason: "session ended",
      });
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

    const sessionData = sessionSnap.data() || {};
    if (isClosedSession(sessionData)) {
      return res.json({ success: true, ignored: true, reason: "session ended" });
    }

    const cachedLive = liveTelemetryCache.get(sessionId);
    const currentLatestTelemetry =
      cachedLive?.latestTelemetry ?? sessionData.latestTelemetry;

    if (!isNewerTelemetrySample(latestTelemetry, currentLatestTelemetry)) {
      return res.json({
        success: true,
        ignored: true,
        reason: "old or duplicate telemetry",
      });
    }

    const latestMapPosition = buildLatestMapPosition(latestTelemetry);
    const livePayload = buildLiveTelemetryPayload(
      sessionId,
      latestTelemetry,
      latestMapPosition,
      sessionData
    );

    broadcastLiveTelemetry(sessionId, livePayload);

    const nowMs = Date.now();
    const lastFirestoreWriteMs =
      latestFirestoreWriteAtBySession.get(sessionId) || 0;
    const shouldWriteFirestore =
      nowMs - lastFirestoreWriteMs >= LATEST_FIRESTORE_WRITE_INTERVAL_MS;

    if (!shouldWriteFirestore) {
      return res.json({
        success: true,
        live: true,
        firestoreSkipped: true,
        hasMapPosition: !!latestMapPosition,
      });
    }

    const updateBody = {
      latestTelemetry,
      latestTelemetryFreshness: telemetryFreshness(latestTelemetry),
      latestTelemetryAt: FieldValue.serverTimestamp(),
    };

    if (latestMapPosition) {
      updateBody.latestMapPosition = latestMapPosition;
      updateBody["mapSummary.latestMapPosition"] = latestMapPosition;
      updateBody["mapSummary.hasWorldPosition"] = true;
    }

    await sessionRef.set(stripUndefinedDeep(updateBody), { merge: true });
    latestFirestoreWriteAtBySession.set(sessionId, nowMs);

    res.json({ success: true, live: true, hasMapPosition: !!latestMapPosition });
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
    if (isClosedSession(sessionData)) {
      return res.json({
        success: true,
        ignored: true,
        reason: "session ended",
        count: 0,
      });
    }

    const stats = buildMapStats(samples);
    const latestTelemetry = samples[samples.length - 1];
    const latestMapPosition = stats.latestMapPosition ?? buildLatestMapPosition(latestTelemetry);
    const cachedLive = liveTelemetryCache.get(sessionId);
    const currentLatestTelemetry =
      cachedLive?.latestTelemetry ?? sessionData.latestTelemetry;
    const shouldUpdateLatest = isNewerTelemetrySample(
      latestTelemetry,
      currentLatestTelemetry
    );
    const acceptedLatestMapPosition = shouldUpdateLatest
      ? latestMapPosition
      : sessionData.latestMapPosition ?? sessionData.mapSummary?.latestMapPosition ?? null;

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
      acceptedLatestMapPosition,
      sessionData
    );

    const updateBody = {
      processedSummary: mergedSummary,
      mapSummary: mergedMapSummary,
    };

    if (shouldUpdateLatest) {
      const livePayload = buildLiveTelemetryPayload(
        sessionId,
        latestTelemetry,
        latestMapPosition,
        sessionData
      );
      broadcastLiveTelemetry(sessionId, livePayload);

      updateBody.latestTelemetry = latestTelemetry;
      updateBody.latestTelemetryFreshness = telemetryFreshness(latestTelemetry);
      updateBody.latestTelemetryAt = FieldValue.serverTimestamp();
      latestFirestoreWriteAtBySession.set(sessionId, Date.now());

      if (latestMapPosition) {
        updateBody.latestMapPosition = latestMapPosition;
      }
    }

    await sessionRef.set(stripUndefinedDeep(updateBody), { merge: true });

    const trackKey = await mergeGlobalTrackMap(sessionData, stats, acceptedLatestMapPosition);

    res.status(201).json({
      success: true,
      count: samples.length,
      mapPointCount: stats.mapPointCount,
      latestAccepted: shouldUpdateLatest,
      trackKey,
      mapBounds: stats.bounds,
    });
  } catch (err) {
    console.error("POST /telemetry/batch error:", err);
    res.status(500).json({ error: "failed to save telemetry batch" });
  }
});

app.post("/sessions/:id/track-map/finalize", authenticate, requireAdmin, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const requestedMaxPoints = parseInteger(req.body.maxPoints, 500) || 500;
    const maxPoints = Math.max(100, Math.min(requestedMaxPoints, 800));

    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();
    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const sessionData = sessionSnap.data() || {};
    const points = await collectSessionMapPoints(sessionRef, {
      maxPoints: Math.max(maxPoints * 3, 1000),
    });

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
      sourceSampleCount: points.rawPointCount ?? points.length,
      centerlinePointCount: centerline.length,
      centerlineVersion: version,
      finalizedAt: FieldValue.serverTimestamp(),
    });

    await sessionRef.set(
      {
        trackMap,
        mapSummary: mergeMapSummary(sessionData.mapSummary || {}, {
          mapPointCount: points.rawPointCount ?? points.length,
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
        sourceSampleCount: points.rawPointCount ?? points.length,
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
      sourceSampleCount: points.rawPointCount ?? points.length,
      centerlinePointCount: centerline.length,
      centerlineChunkCount: chunkCount,
      centerlineVersion: version,
    });
  } catch (err) {
    console.error("POST /sessions/:id/track-map/finalize error:", err);
    res.status(500).json({ error: "failed to finalize track map" });
  }
});

app.get("/sessions/:id/live-stream", authenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }

    const sessionData = sessionSnap.data() || {};
    if (!requireReadableSession(req, res, sessionData)) return;

    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders?.();
    res.socket?.setTimeout?.(0);

    const clients = getLiveClientSet(sessionId);
    clients.add(res);

    writeLiveStreamEvent(res, "ready", {
      sessionId,
      serverSentAt: new Date().toISOString(),
    });

    const cachedPayload = liveTelemetryCache.get(sessionId);
    const initialPayload =
      cachedPayload ||
      buildLiveTelemetryPayload(
        sessionId,
        sessionData.latestTelemetry ?? null,
        sessionData.latestMapPosition ?? sessionData.mapSummary?.latestMapPosition ?? null,
        sessionData
      );

    if (initialPayload.latestTelemetry) {
      writeLiveStreamEvent(res, "telemetry", initialPayload);
    }

    const heartbeat = setInterval(() => {
      if (res.destroyed || res.writableEnded) return;
      writeLiveStreamEvent(res, "ping", {
        sessionId,
        serverSentAt: new Date().toISOString(),
      });
    }, LIVE_STREAM_HEARTBEAT_MS);

    res.on("close", () => {
      clearInterval(heartbeat);
      clients.delete(res);
      if (clients.size === 0) {
        liveStreamClients.delete(sessionId);
      }
    });
  } catch (err) {
    console.error("GET /sessions/:id/live-stream error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "failed to open live telemetry stream" });
    } else {
      res.end();
    }
  }
});

app.get("/sessions/:id/track-map", authenticate, async (req, res) => {
  try {
    const sessionId = safeString(req.params.id);
    const sessionRef = db.collection("sessions").doc(sessionId);
    const sessionSnap = await sessionRef.get();

    if (!sessionSnap.exists) {
      return res.status(404).json({ error: "session not found" });
    }
    if (!requireReadableSession(req, res, sessionSnap.data() || {})) return;

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

app.patch("/track-maps/:trackKey/calibration", authenticate, requireAdmin, async (req, res) => {
  try {
    const trackKey = safeString(req.params.trackKey);
    if (!trackKey) return res.status(400).json({ error: "trackKey is required" });

    const imageWidth = parseNumber(req.body.imageWidth, null);
    const imageHeight = parseNumber(req.body.imageHeight, null);
    const imageUrl = safeString(req.body.imageUrl, null);
    const trackId = parseInteger(req.body.trackId, null);
    const trackName = safeString(req.body.trackName, null);

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

    if (anchorPoints.length < 3) {
      return res.status(400).json({ error: "at least 3 anchor points are required" });
    }

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
      stripUndefinedDeep({
        trackKey,
        trackId,
        trackName,
        imageCalibration: calibration,
        updatedAt: FieldValue.serverTimestamp(),
      }),
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
      "emails",
      "authSessions",
      "listenerTokens",
      "sessions",
      "sessions/{sessionId}/telemetryChunks",
      "sessions/{sessionId}/laps",
      "sessions/{sessionId}/corners",
      "sessions/{sessionId}/reports/postSession",
      "sessions/{sessionId}/reports/postSession/laps/{lapId}",
      "trackMaps",
      "trackMaps/{trackKey}/centerlineChunks",
    ],
    importantSessionFields: {
      latestTelemetry: "Latest raw telemetry sample, including worldX/worldY/worldZ.",
      latestMapPosition: "Small map-ready current car position.",
      mapSummary: "Session-level world bounds and map sample count.",
      trackMap: "Finalized map generated from a calibration/session recording.",
      postSessionReport: "Coach Evidence Report stored at sessions/{sessionId}/reports/postSession, with detailed braking/cornering lap docs under reports/postSession/laps.",
    },
    importantTrackMapFields: {
      worldBounds: "min/max world X/Z and calculated width/height in game metres.",
      imageCalibration:
        "Optional image size and anchor points used to align world coordinates to a track image.",
      centerlineChunks:
        "Downsampled points for drawing the track path without overloading one Firestore document.",
    },
    strategy:
      "Sessions store driving data. trackMaps store reusable circuit calibration data for map overlays. Each ended session stores one compact AI coaching report at sessions/{sessionId}/reports/postSession. Admin endpoints manage account status, roles, and recorded session times.",
  });
});

async function start() {
  app.listen(port, () => console.log("API RUNNING on port", port));
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
