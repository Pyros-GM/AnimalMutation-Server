# Animal Mutation Server

Shared-world relay for Animal Mutation. Zero dependencies, runs on Render's free web service.

- `GET /` — health/status JSON (`players` online)
- WebSocket on the same URL (`wss://<service>.onrender.com`) — clients send their position/form, server broadcasts everyone's state 10×/sec.

Run locally: `node server.js` (listens on `$PORT`, default 10000).
