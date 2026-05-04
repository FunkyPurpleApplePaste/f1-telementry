const BASE = "https://f1-telementry-1.onrender.com";

async function test() {
  const p = await fetch(`${BASE}/players`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: "test_player2" })
  });

  console.log("player:", await p.text());

  const s = await fetch(`${BASE}/sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ playerId: "test_player2" })
  });

  const session = await s.json();
  console.log("session:", session);

  const t = await fetch(`${BASE}/telemetry/sample`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sessionId: session.id,
      speedKph: 280,
      throttle: 0.9
    })
  });

  console.log("telemetry:", await t.text());
}

test();