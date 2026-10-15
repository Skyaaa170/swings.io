'use strict';
// Serveur Escadrille : sert le jeu et relaie l'état des joueurs entre eux, salon par salon.
// Chaque joueur envoie son état (~15 fois par seconde) ; le serveur le transmet aux autres joueurs du salon.

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
const MAX_RATE = 60;           // messages max par seconde et par joueur
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const LABEL_RE = /^[a-z2-7]{16}$/;
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json'
};

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

/* ---------- Salons en temps réel ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });
const rooms = new Map(); // nom du salon -> Map(identifiant -> joueur)

function newLabel() {
  const abc = 'abcdefghijklmnopqrstuvwxyz234567';
  let s = '';
  for (const b of crypto.randomBytes(16)) s += abc[b & 31];
  return s;
}
function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
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

wss.on('connection', (ws) => {
  const c = { ws, room: null, label: null, g: null, alive: true, count: 0, windowStart: Date.now() };
  ws.client = c;
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    if (now - c.windowStart >= 1000) { c.windowStart = now; c.count = 0; }
    if (++c.count > MAX_RATE) return;
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join') {
      if (typeof m.room !== 'string' || !ROOM_RE.test(m.room)) { send(ws, { t: 'error', code: 'invalid_argument' }); return; }
      leaveRoom(c);
      let room = rooms.get(m.room);
      if (!room) { room = new Map(); rooms.set(m.room, room); }
      if (room.size >= MAX_PEERS) { send(ws, { t: 'error', code: 'limit_reached' }); return; }
      // Après une coupure, le joueur récupère son identifiant s'il est libre
      const label = typeof m.want === 'string' && LABEL_RE.test(m.want) && !room.has(m.want) ? m.want : newLabel();
      c.label = label; c.room = m.room; c.g = null;
      const peers = [];
      for (const o of room.values()) if (o.g) peers.push({ peer: o.label, g: o.g });
      room.set(label, c);
      send(ws, { t: 'joined', me: label, peers });
    } else if (m.t === 'presence') {
      if (!c.room || !m.g || typeof m.g !== 'object' || Array.isArray(m.g)) return;
      const json = JSON.stringify(m.g);
      if (json.length > MAX_PRESENCE) return;
      c.g = m.g;
      const room = rooms.get(c.room);
      if (!room) return;
      const data = '{"t":"p","peer":"' + c.label + '","g":' + json + '}';
      for (const o of room.values()) if (o !== c) sendRaw(o, data);
    } else if (m.t === 'leave') {
      leaveRoom(c);
    }
  });
  ws.on('close', () => leaveRoom(c));
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

server.listen(PORT, () => {
  console.log('Escadrille est prêt !');
  console.log('  Sur cet ordinateur : http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      if (a.family === 'IPv4' && !a.internal) console.log('  Sur le même Wi-Fi : http://' + a.address + ':' + PORT);
    }
  }
});
