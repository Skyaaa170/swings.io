'use strict';
// Serveur Escadrille : sert le jeu, relaie les parties salon par salon,
// gère les codes amis, les invitations et le classement général.

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC = path.join(__dirname, 'public');
const MAX_MSG = 6000;          // taille max d'un message reçu (octets)
const MAX_PRESENCE = 4096;     // taille max de l'état d'un joueur
const MAX_PEERS = 16;          // joueurs max par salon
const MAX_RATE = 60;           // messages max par seconde et par connexion
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const CODE_RE = /^[a-z0-9][a-z0-9_.-]{0,40}$/;
const LABEL_RE = /^[a-z2-7]{16}$/;
const ID_RE = /^[A-Z2-7]{8}$/;
const SECRET_RE = /^[a-z2-7]{26,40}$/;
const B32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
const SKIN_LIMITS = [3, 12, 8, 5, 4]; // modèle, couleur, détails, motif, traînée
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json'
};

/* ---------- Sauvegarde (Upstash si configuré, sinon fichier local) ---------- */
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').trim().replace(/\/+$/, '');
const UP_TOKEN = (process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
const USE_UPSTASH = !!(UP_URL && UP_TOKEN);
const DB_KEY = 'escadrille:db';
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'db.json');
let db = { v: 1, players: {} };
let loaded = false, dirty = false, saving = false, lbCache = {};

async function readStore() {
  if (USE_UPSTASH) {
    const r = await fetch(UP_URL + '/get/' + encodeURIComponent(DB_KEY), { headers: { Authorization: 'Bearer ' + UP_TOKEN } });
    if (!r.ok) throw new Error('Upstash HTTP ' + r.status);
    const j = await r.json();
    return typeof j.result === 'string' ? j.result : null;
  }
  return fs.existsSync(DATA_FILE) ? fs.readFileSync(DATA_FILE, 'utf8') : null;
}
async function writeStore(json) {
  if (USE_UPSTASH) {
    const r = await fetch(UP_URL, {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UP_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(['SET', DB_KEY, json])
    });
    if (!r.ok) throw new Error('Upstash HTTP ' + r.status);
    return;
  }
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE + '.tmp', json);
  fs.renameSync(DATA_FILE + '.tmp', DATA_FILE);
}
// Si la lecture échoue au démarrage, on joue quand même : les statistiques récoltées entre-temps
// sont ajoutées à la sauvegarde dès qu'elle redevient lisible (rien n'est écrasé).
async function loadDb() {
  try {
    const raw = await readStore();
    const saved = raw ? JSON.parse(raw) : { v: 1, players: {} };
    if (!saved || typeof saved.players !== 'object') throw new Error('sauvegarde illisible');
    for (const [id, mem] of Object.entries(db.players)) {
      const old = saved.players[id];
      if (!old) { saved.players[id] = mem; continue; }
      old.games += mem.games; old.kills += mem.kills; old.total += mem.total;
      old.best = Math.max(old.best, mem.best);
      if (mem.t >= old.t) { old.n = mem.n; old.k = mem.k; old.t = mem.t; }
    }
    db = saved; loaded = true; lbCache = {};
    console.log('Sauvegarde : ' + (USE_UPSTASH ? 'Upstash' : 'fichier ' + DATA_FILE) + ' (' + Object.keys(db.players).length + ' pilotes)');
  } catch (e) {
    console.error('Lecture de la sauvegarde impossible, nouvel essai bientôt :', e.message);
  }
}
async function saveDb() {
  if (saving) return;
  if (!loaded) { await loadDb(); if (!loaded) return; dirty = true; }
  if (!dirty) return;
  saving = true; dirty = false;
  try { await writeStore(JSON.stringify(db)); }
  catch (e) { dirty = true; console.error('Sauvegarde impossible :', e.message); }
  saving = false;
}
setInterval(saveDb, 10000);
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => { await saveDb(); process.exit(0); });
}

/* ---------- Joueurs ---------- */
const cleanName = (v) => (typeof v === 'string' ? v.replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g, '').trim().slice(0, 16) : '') || 'Pilote';
const cleanSkin = (k) => SKIN_LIMITS.map((lim, i) => {
  const v = Array.isArray(k) ? Math.floor(Number(k[i])) : 0;
  return Number.isFinite(v) ? Math.min(lim - 1, Math.max(0, v)) : 0;
});
const int = (v, max) => { const n = Math.floor(Number(v)); return Number.isFinite(n) ? Math.min(max, Math.max(0, n)) : 0; };
function idFromSecret(secret) {
  const h = crypto.createHash('sha256').update('escadrille:' + secret).digest();
  let s = '';
  for (let i = 0; i < 8; i++) s += B32[h[i] & 31];
  return s;
}
function touchPlayer(id, name, skin) {
  let rec = db.players[id];
  if (!rec) {
    rec = db.players[id] = { n: name, k: skin, best: 0, kills: 0, games: 0, total: 0, t: Date.now() };
    dirty = true;
    prunePlayers();
  } else if (rec.n !== name || String(rec.k) !== String(skin)) {
    rec.n = name; rec.k = skin; dirty = true; lbCache = {};
  }
  rec.t = Date.now();
  return rec;
}
function prunePlayers() {
  const ids = Object.keys(db.players);
  if (ids.length <= 5000) return;
  const idle = ids.filter((id) => !db.players[id].games).sort((a, b) => db.players[a].t - db.players[b].t);
  for (const id of idle.slice(0, ids.length - 4500)) delete db.players[id];
}
function leaderboard(sort) {
  if (!lbCache[sort]) {
    lbCache[sort] = Object.entries(db.players)
      .filter(([, r]) => r.games > 0 && r[sort] > 0)
      .sort((a, b) => b[1][sort] - a[1][sort] || a[1].t - b[1].t)
      .map(([id, r]) => ({ id, n: r.n, k: r.k, v: r[sort] }));
  }
  return lbCache[sort];
}

/* ---------- Fichiers du jeu ---------- */
const server = http.createServer((req, res) => {
  let url;
  try { url = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); res.end(); return; }
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/') url = '/index.html';
  const file = path.normalize(path.join(PUBLIC, url));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('Introuvable'); return; }
    res.writeHead(200, { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-cache' });
    res.end(data);
  });
});

/* ---------- Connexions en temps réel ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });
const rooms = new Map();  // nom du salon -> Map(identifiant de salon -> connexion)
const online = new Map(); // code ami -> Set(connexions)

function newLabel() {
  const abc = 'abcdefghijklmnopqrstuvwxyz234567';
  let s = '';
  for (const b of crypto.randomBytes(16)) s += abc[b & 31];
  return s;
}
function send(c, msg) {
  if (c.ws.readyState === 1) c.ws.send(JSON.stringify(msg));
}
function sendRaw(c, data) {
  // Un joueur à la connexion trop lente ne bloque pas les autres : on saute ses envois en retard
  if (c.ws.readyState === 1 && c.ws.bufferedAmount < 512 * 1024) c.ws.send(data);
}
function leaveRoom(c) {
  if (!c.room) return;
  const room = rooms.get(c.room);
  if (room && room.get(c.label) === c) {
    room.delete(c.label);
    const data = JSON.stringify({ t: 'left', peer: c.label });
    for (const o of room.values()) sendRaw(o, data);
    if (!room.size) rooms.delete(c.room);
  }
  c.room = null;
}
function setIdentity(c, id) {
  if (c.id === id) return;
  if (c.id) { const set = online.get(c.id); if (set) { set.delete(c); if (!set.size) online.delete(c.id); } }
  c.id = id;
  if (id) { if (!online.has(id)) online.set(id, new Set()); online.get(id).add(c); }
}
function statusOf(id) {
  const set = online.get(id);
  const rec = db.players[id];
  let first = null, room = null;
  if (set) for (const o of set) { if (!first) first = o; if (!room && o.room && o.room.startsWith('esc-')) room = o.room.slice(4); }
  return { id, on: !!first, n: first ? first.name : rec ? rec.n : null, k: first ? first.skin : rec ? rec.k : null, room };
}

const handlers = {
  hello(c, m) {
    if (typeof m.secret !== 'string' || !SECRET_RE.test(m.secret)) return;
    setIdentity(c, idFromSecret(m.secret));
    c.name = cleanName(m.n); c.skin = cleanSkin(m.k);
    const rec = touchPlayer(c.id, c.name, c.skin);
    send(c, { t: 'welcome', id: c.id, stats: { best: rec.best, kills: rec.kills, games: rec.games } });
  },
  profile(c, m) {
    if (!c.id) return;
    c.name = cleanName(m.n); c.skin = cleanSkin(m.k);
    touchPlayer(c.id, c.name, c.skin);
  },
  join(c, m) {
    if (typeof m.room !== 'string' || !ROOM_RE.test(m.room)) { send(c, { t: 'error', ctx: 'join', code: 'invalid_argument' }); return; }
    leaveRoom(c);
    let room = rooms.get(m.room);
    if (!room) { room = new Map(); rooms.set(m.room, room); }
    if (room.size >= MAX_PEERS) { send(c, { t: 'error', ctx: 'join', code: 'limit_reached' }); if (!room.size) rooms.delete(m.room); return; }
    // Après une coupure, le joueur récupère son identifiant de salon s'il est libre
    const label = typeof m.want === 'string' && LABEL_RE.test(m.want) && !room.has(m.want) ? m.want : newLabel();
    c.label = label; c.room = m.room; c.g = null;
    const peers = [];
    for (const o of room.values()) if (o.g) peers.push({ peer: o.label, g: o.g });
    room.set(label, c);
    send(c, { t: 'joined', me: label, peers });
  },
  presence(c, m) {
    if (!c.room || !m.g || typeof m.g !== 'object' || Array.isArray(m.g)) return;
    const json = JSON.stringify(m.g);
    if (json.length > MAX_PRESENCE) return;
    c.g = m.g;
    const room = rooms.get(c.room);
    if (!room) return;
    const data = '{"t":"p","peer":"' + c.label + '","g":' + json + '}';
    for (const o of room.values()) if (o !== c) sendRaw(o, data);
  },
  leave(c) { leaveRoom(c); },
  friends(c, m) {
    if (!Array.isArray(m.ids)) return;
    const list = m.ids.slice(0, 100).filter((id) => typeof id === 'string' && ID_RE.test(id)).map(statusOf);
    send(c, { t: 'friends', list });
  },
  addFriend(c, m, now) {
    if (!c.id || now - c.lastAdd < 800) return;
    c.lastAdd = now;
    const id = typeof m.id === 'string' && ID_RE.test(m.id) ? m.id : null;
    if (!id || id === c.id) { send(c, { t: 'friendRes', id, ok: false }); return; }
    const st = statusOf(id);
    const known = st.on || !!db.players[id];
    send(c, { t: 'friendRes', id, ok: known, n: st.n });
    const set = online.get(id);
    if (set) for (const o of set) send(o, { t: 'friendAdded', from: { id: c.id, n: c.name } });
  },
  invite(c, m, now) {
    if (!c.id || now - c.lastInvite < 1500) return;
    c.lastInvite = now;
    if (typeof m.to !== 'string' || !ID_RE.test(m.to) || typeof m.room !== 'string' || !CODE_RE.test(m.room)) return;
    const set = online.get(m.to);
    if (set) for (const o of set) send(o, { t: 'invite', from: { id: c.id, n: c.name }, room: m.room });
    send(c, { t: 'inviteRes', to: m.to, ok: !!set });
  },
  result(c, m, now) {
    if (!c.id || now - c.lastResult < 2000) return;
    c.lastResult = now;
    const score = int(m.score, 200000), kills = int(m.kills, 1000), time = int(m.time, 6 * 3600);
    if (time < 3 && score === 0) return;
    const rec = touchPlayer(c.id, c.name, c.skin);
    rec.games++; rec.kills += kills; rec.total += score;
    if (score > rec.best) rec.best = score;
    dirty = true; lbCache = {};
    const rank = 1 + Object.values(db.players).filter((r) => r.games > 0 && r.best > rec.best).length;
    send(c, { t: 'resultOk', best: rec.best, kills: rec.kills, rank });
  },
  lb(c, m) {
    const sort = m.sort === 'kills' ? 'kills' : 'best';
    const list = leaderboard(sort);
    const rows = list.slice(0, 50).map((r) => ({ n: r.n, k: r.k, v: r.v, me: r.id === c.id }));
    let me = null;
    if (c.id) { const i = list.findIndex((r) => r.id === c.id); if (i >= 0) me = { rank: i + 1, v: list[i].v }; }
    send(c, { t: 'lb', sort, rows, me, total: list.length });
  }
};

wss.on('connection', (ws) => {
  const c = { ws, id: null, name: 'Pilote', skin: [0, 0, 0, 0, 0], room: null, label: null, g: null, alive: true,
    count: 0, windowStart: Date.now(), lastAdd: 0, lastInvite: 0, lastResult: 0 };
  ws.client = c;
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    if (now - c.windowStart >= 1000) { c.windowStart = now; c.count = 0; }
    if (++c.count > MAX_RATE) return;
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string' || !Object.prototype.hasOwnProperty.call(handlers, m.t)) return;
    handlers[m.t](c, m, now);
  });
  ws.on('close', () => { leaveRoom(c); setIdentity(c, null); });
  ws.on('error', () => { /* la fermeture suit */ });
});

// Déconnecte les joueurs qui ne répondent plus
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    const c = ws.client;
    if (!c.alive) { ws.terminate(); continue; }
    c.alive = false;
    try { ws.ping(); } catch (e) { /* ignoré */ }
  }
}, 20000);
wss.on('close', () => clearInterval(heartbeat));

loadDb().then(() => {
  server.listen(PORT, () => {
    console.log('Escadrille est prêt !');
    console.log('  Sur cet ordinateur : http://localhost:' + PORT);
    for (const list of Object.values(os.networkInterfaces())) {
      for (const a of list || []) {
        if (a.family === 'IPv4' && !a.internal) console.log('  Sur le même Wi-Fi : http://' + a.address + ':' + PORT);
      }
    }
    if (!USE_UPSTASH) console.log('  Classement sauvegardé dans un fichier local (voir LISEZMOI pour Render).');
  });
});
