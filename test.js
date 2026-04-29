const fetch = require("node-fetch");

const BASE = "https://f1-telementry-1.onrender.com";

async function test() {
  // Create player
  await fetch(`${BASE}/players`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: "test_player2" })
  });

  // Create session
  const sessionRes = await fetch(`${BASE}/sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ playerId: "test_player2" })
  });

  const session = await sessionRes.json();

  // Send telemetry
  await fetch(`${BASE}/telemetry/sample`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sessionId: session.id,
      speedKph: 280,
      throttle: 0.9
    })
  });

  console.log("Done");
}

test();