// Animal Mutation shared-world server.
// Zero dependencies: plain Node http + a minimal WebSocket implementation (text frames only).
//
// Protocol (JSON text frames):
//   client -> server
//     {"t":"hello","name":"Rex","form":"hatchling"}
//     {"t":"state","p":[x,y,z],"r":yaw,"f":"form_id","l":level,"d":dead,"hp":hp,"mh":maxHp,"v":pvpOn,"h":hidden}
//     {"t":"hit","to":id,"d":dmg,"p":poisonDps,"pd":poisonSecs,"s":stunSecs,"sl":[slowFrac,secs],"kb":[x,y,z]}
//     {"t":"fx","k":"kind","n":"Skill Name","c":"#rrggbb","r":radius}
//     {"t":"died"}
//   shared enemies (each regular enemy is run by one player's game, its owner):
//     {"t":"en","m":map,"list":[{k,t,l,p,r,hp,mh,ak}, ...]}   owner -> others on that map
//     {"t":"ehit","to":ownerId,"k":key,"d":..,"p":..,"pd":..,"s":..,"kb":[..],"kd":..,"db":[name,a,b]}
//     {"t":"pvehit","to":playerId,"d":dmg,"k":key}           owner's enemy bit another player
//     {"t":"edie","k":key,"lv":level,"xb":xpBase,"hit":[ids]}
//   server -> client
//     {"t":"welcome","id":3,"players":[{id,name,p,r,f,l,d}, ...]}
//     {"t":"join","id":4,"name":"Kiwi"}
//     {"t":"leave","id":4}
//     {"t":"states","list":[{id,p,r,f,l,d,hp,mh,v,h}, ...]}      (10 times a second)
//     {"t":"hit","from":id, ...same fields as above}       (only to the target; both must have PvP on)
//     {"t":"fx","id":id,"k":..,"n":..,"c":..,"r":..}
//     {"t":"kill","id":victim,"by":killer or -1,"name":..,"byName":..}

const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 10000;
const MAX_PLAYERS = 32;
const TICK_MS = 100;
const IDLE_TIMEOUT_MS = 30000;
const HIT_RANGE = 45;          // generous: positions are 100 ms old
const MAX_HITS_PER_SEC = 120;   // hits + enemy hits + debuffs, per player
const MAX_ENEMIES = 40;
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

let nextId = 1;
const players = new Map(); // id -> {socket, name, p, r, f, l, d, last, buf}

// ---------------- HTTP (health check / wake-up ping) ----------------
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify({ game: "Animal Mutation", ok: true, players: players.size, max: MAX_PLAYERS }));
});

// ---------------- WebSocket upgrade ----------------
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers.upgrade || "").toLowerCase() !== "websocket") {
    socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
    return;
  }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);

  if (players.size >= MAX_PLAYERS) {
    sendRaw(socket, JSON.stringify({ t: "full" }));
    closeSocket(socket, 1013);
    return;
  }

  const id = nextId++;
  const pl = { socket, id, name: "", p: [0, 0, 0], r: 0, f: "", l: 1, d: false, hp: 100, mh: 100, v: true, h: false, m: 0, a: false, hello: false, lastHitBy: 0, lastHitAt: 0, hitWin: 0, hitCount: 0, last: Date.now(), buf: Buffer.alloc(0) };
  players.set(id, pl);

  socket.on("data", (chunk) => {
    pl.buf = Buffer.concat([pl.buf, chunk]);
    if (pl.buf.length > 1 << 20) return drop(pl);
    let frame;
    while ((frame = readFrame(pl)) !== null) {
      pl.last = Date.now();
      if (frame.op === 0x8) return drop(pl); // close
      if (frame.op === 0x9) { sendRaw(socket, frame.data, 0xA); continue; } // ping -> pong
      if (frame.op === 0x1) onMessage(pl, frame.data.toString("utf8"));
    }
  });
  socket.on("close", () => drop(pl));
  socket.on("error", () => drop(pl));
});

// ---------------- game messages ----------------
function onMessage(pl, text) {
  let m;
  try { m = JSON.parse(text); } catch { return; }
  if (!m || typeof m !== "object") return;
  if (m.t === "hello" && !pl.hello) {
    pl.hello = true;
    pl.name = String(m.name || "Creature").replace(/[^\w \-]/g, "").slice(0, 16) || "Creature";
    pl.f = String(m.form || "").slice(0, 48);
    const others = [];
    for (const o of players.values()) if (o !== pl && o.hello) others.push(pub(o, true));
    send(pl, { t: "welcome", id: pl.id, players: others });
    broadcast({ t: "join", id: pl.id, name: pl.name, f: pl.f }, pl);
    console.log(`join #${pl.id} ${pl.name} (${players.size} online)`);
  } else if (m.t === "state" && pl.hello) {
    if (Array.isArray(m.p) && m.p.length === 3 && m.p.every(Number.isFinite)) pl.p = m.p.map((v) => Math.round(v * 100) / 100);
    if (Number.isFinite(m.r)) pl.r = Math.round(m.r * 100) / 100;
    if (typeof m.f === "string") pl.f = m.f.slice(0, 48);
    if (Number.isFinite(m.l)) pl.l = m.l | 0;
    pl.d = !!m.d;
    if (Number.isFinite(m.hp)) pl.hp = Math.round(m.hp);
    if (Number.isFinite(m.mh)) pl.mh = Math.round(m.mh);
    if (typeof m.v === "boolean") pl.v = m.v;
    pl.h = !!m.h;
    if (Number.isFinite(m.m)) pl.m = m.m | 0;
    pl.a = !!m.a;
  } else if (m.t === "hit" && pl.hello) {
    const tgt = players.get(m.to | 0);
    if (!tgt || !tgt.hello || tgt === pl || !pl.v || !tgt.v || pl.d || tgt.d) return;
    if (dist(pl.p, tgt.p) > HIT_RANGE) return;
    const now = Date.now();
    if (!rate(pl)) return;
    const out = { t: "hit", from: pl.id, d: num(m.d, 0, 5000) };
    if (m.p) { out.p = num(m.p, 0, 500); out.pd = num(m.pd, 0, 15); }
    if (m.s) out.s = num(m.s, 0, 3);
    if (Array.isArray(m.sl) && m.sl.length === 2) out.sl = [num(m.sl[0], 0, 0.9), num(m.sl[1], 0, 6)];
    if (Array.isArray(m.kb) && m.kb.length === 3) out.kb = m.kb.map((v) => num(v, -40, 40));
    tgt.lastHitBy = pl.id;
    tgt.lastHitAt = now;
    send(tgt, out);
  } else if (m.t === "en" && pl.hello && Array.isArray(m.list)) {
    // Owner's enemies: only to players on the same map.
    const list = m.list.slice(0, MAX_ENEMIES).filter((e) => e && typeof e === "object").map((e) => ({
      k: e.k | 0, t: String(e.t || "").slice(0, 24), l: e.l | 0,
      p: Array.isArray(e.p) && e.p.length === 3 ? e.p.map((v) => num(v, -1e4, 1e4)) : [0, 0, 0],
      r: num(e.r, -100, 100), hp: num(e.hp, 0, 1e6), mh: num(e.mh, 1, 1e6), ak: e.ak | 0,
    }));
    sameMap(pl, { t: "en", id: pl.id, m: pl.m, list });
  } else if (m.t === "ehit" && pl.hello) {
    const owner = players.get(m.to | 0);
    if (!owner || !owner.hello || owner === pl || owner.m !== pl.m || !rate(pl)) return;
    const out = { t: "ehit", from: pl.id, k: m.k | 0, d: num(m.d, 0, 5000) };
    if (m.p) { out.p = num(m.p, 0, 500); out.pd = num(m.pd, 0, 15); }
    if (m.s) out.s = num(m.s, 0, 3);
    if (Array.isArray(m.kb) && m.kb.length === 3) { out.kb = m.kb.map((v) => num(v, -40, 40)); out.kd = num(m.kd, 0, 2); }
    if (Array.isArray(m.db) && m.db.length >= 2 && m.db.length <= 3 && typeof m.db[0] === "string")
      out.db = [m.db[0].slice(0, 20), ...m.db.slice(1).map((v) => num(v, 0, 30))];
    send(owner, out);
  } else if (m.t === "pvehit" && pl.hello) {
    const tgt = players.get(m.to | 0);
    if (!tgt || !tgt.hello || tgt === pl || tgt.m !== pl.m || tgt.d || !rate(pl)) return;
    send(tgt, { t: "pvehit", from: pl.id, k: m.k | 0, d: num(m.d, 0, 2000) });
  } else if (m.t === "edie" && pl.hello) {
    const hit = Array.isArray(m.hit) ? m.hit.slice(0, 32).map((v) => v | 0) : [];
    sameMap(pl, { t: "edie", id: pl.id, k: m.k | 0, lv: m.lv | 0, xb: num(m.xb, 0, 1e5), hit });
  } else if (m.t === "fx" && pl.hello) {
    broadcast({ t: "fx", id: pl.id, k: String(m.k || "").slice(0, 24), n: String(m.n || "").slice(0, 32), c: String(m.c || "").slice(0, 10), r: num(m.r, 0, 30) }, pl);
  } else if (m.t === "died" && pl.hello) {
    const killer = pl.lastHitBy && Date.now() - pl.lastHitAt < 15000 ? players.get(pl.lastHitBy) : null;
    pl.lastHitBy = 0;
    broadcast({ t: "kill", id: pl.id, by: killer ? killer.id : -1, name: pl.name, byName: killer ? killer.name : "" });
    console.log(`kill #${pl.id} ${pl.name} by ${killer ? killer.name : "the wild"}`);
  }
}

// Per-player cap on hit-type messages.
function rate(pl) {
  const now = Date.now();
  if (now - pl.hitWin > 1000) { pl.hitWin = now; pl.hitCount = 0; }
  return ++pl.hitCount <= MAX_HITS_PER_SEC;
}

function sameMap(pl, obj) {
  const txt = JSON.stringify(obj);
  for (const o of players.values()) if (o !== pl && o.hello && o.m === pl.m) sendRaw(o.socket, txt);
}

function num(v, lo, hi) {
  return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : 0;
}

function dist(a, b) {
  return Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
}

function pub(o, withName) {
  const s = { id: o.id, p: o.p, r: o.r, f: o.f, l: o.l, d: o.d, hp: o.hp, mh: o.mh, v: o.v, h: o.h, m: o.m, a: o.a };
  if (withName) s.name = o.name;
  return s;
}

function drop(pl) {
  if (!players.has(pl.id)) return;
  players.delete(pl.id);
  try { pl.socket.destroy(); } catch {}
  if (pl.hello) {
    broadcast({ t: "leave", id: pl.id });
    console.log(`leave #${pl.id} ${pl.name} (${players.size} online)`);
  }
}

function send(pl, obj) { sendRaw(pl.socket, JSON.stringify(obj)); }
function broadcast(obj, except) {
  const txt = JSON.stringify(obj);
  for (const o of players.values()) if (o !== except && o.hello) sendRaw(o.socket, txt);
}

// 10 Hz: everyone gets everyone else's state; drop silent sockets.
setInterval(() => {
  const now = Date.now();
  const list = [];
  for (const o of players.values()) {
    if (now - o.last > IDLE_TIMEOUT_MS) { drop(o); continue; }
    if (o.hello) list.push(pub(o, false));
  }
  if (list.length < 2) return;
  for (const o of players.values()) {
    if (!o.hello) continue;
    sendRaw(o.socket, JSON.stringify({ t: "states", list: list.filter((s) => s.id !== o.id) }));
  }
}, TICK_MS);

// ---------------- WebSocket framing ----------------
function readFrame(pl) {
  const b = pl.buf;
  if (b.length < 2) return null;
  const op = b[0] & 0x0f;
  const masked = (b[1] & 0x80) !== 0;
  let len = b[1] & 0x7f;
  let off = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
  else if (len === 127) { if (b.length < 10) return null; len = Number(b.readBigUInt64BE(2)); off = 10; }
  const maskOff = off;
  if (masked) off += 4;
  if (b.length < off + len) return null;
  let data = b.subarray(off, off + len);
  if (masked) {
    const mask = b.subarray(maskOff, maskOff + 4);
    data = Buffer.from(data);
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
  }
  pl.buf = b.subarray(off + len);
  return { op, data };
}

function sendRaw(socket, payload, op = 0x1) {
  if (socket.destroyed) return;
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, "utf8");
  let head;
  if (data.length < 126) head = Buffer.from([0x80 | op, data.length]);
  else if (data.length < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(data.length, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2); }
  try { socket.write(Buffer.concat([head, data])); } catch {}
}

function closeSocket(socket, code) {
  const d = Buffer.alloc(2); d.writeUInt16BE(code, 0);
  sendRaw(socket, d, 0x8);
  socket.end();
}

server.listen(PORT, () => console.log(`Animal Mutation server listening on ${PORT}`));
