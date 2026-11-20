# Animal Mutation Server

Shared-world relay for Animal Mutation. Zero dependencies, runs on Render's free web service.

- `GET /` — health/status JSON (`players` online)
- WebSocket on the same URL (`wss://<service>.onrender.com`) — clients send their position/form, server broadcasts everyone's state 10×/sec.

PvP hits are only relayed between players on the same map, outside the safe zones (24 m around each map's centre and passage mouths) and at most 10 levels apart (no limit when both are Lv 50+). These match the game's own checks in `gameplay_rules.gd`.

Run locally: `node server.js` (listens on `$PORT`, default 10000).
