// Логово дракона — сервер сетевого лобби.
// Комнаты с кодом из 4 цифр, места игроков и ИИ, готовность, старт партии.
// Во время партии сервер раздаёт всем одно и то же зерно случайности и рассылает действия игроков
// в едином порядке (seq). Каждый телефон считает игру сам — одинаково у всех.
'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');
// политика конфиденциальности: https://<сервер>/privacy
let PRIVACY = '';
try { PRIVACY = fs.readFileSync(path.join(__dirname, 'privacy.html'), 'utf8'); } catch (e) { PRIVACY = '<p>privacy.html не найден</p>'; }

const PORT = process.env.PORT || 8080;
const GRACE_MS = +(process.env.GRACE_MS || 30000);       // сколько ждать переподключения игрока в партии
const LOBBY_GRACE_MS = +(process.env.LOBBY_GRACE_MS || 15000);
const ROOM_IDLE_MS = 10 * 60 * 1000;                      // пустая комната удаляется через 10 минут
const MAX_SEATS = 4;
const BOT_NAMES = ['Ульф', 'Ирма', 'Борг', 'Сельма'];
const MAPS = ['small', 'medium', 'large'];
const PORTRAITS = 8;

// метка этой копии сервера: если при обновлении страницы она меняется — копий несколько
const INSTANCE = crypto.randomBytes(3).toString('hex');
const clients = new Map();   // id -> client
const rooms = new Map();     // code -> room

const now = () => Date.now();
const rid = () => crypto.randomBytes(9).toString('base64url');
const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);

function newCode() {
  for (let k = 0; k < 1000; k++) {
    const c = String(crypto.randomInt(1000, 10000));   // код комнаты — 4 цифры, легко продиктовать
    if (!rooms.has(c)) return c;
  }
  throw new Error('no code');
}
function send(cl, msg) {
  if (cl && cl.ws && cl.ws.readyState === 1) {
    try { cl.ws.send(JSON.stringify(msg)); } catch (e) { /* ignore */ }
  }
}
function roomView(room) {
  return {
    code: room.code, host: room.host, map: room.map, playing: !!room.game,
    slots: room.slots.map(s => s && (s.bot
      ? { bot: true, name: s.name, portrait: s.portrait }
      : { id: s.id, name: s.name, portrait: s.portrait, ready: !!s.ready, online: !!(clients.get(s.id) || {}).ws })),
  };
}
function broadcastRoom(room) {
  const v = roomView(room);
  room.slots.forEach(s => { if (s && !s.bot) send(clients.get(s.id), { t: 'room', room: v }); });
}
function freePortrait(room, want, exceptSlot) {
  const used = new Set(room.slots.filter((s, i) => s && i !== exceptSlot).map(s => s.portrait));
  if (want >= 0 && want < PORTRAITS && !used.has(want)) return want;
  for (let k = 0; k < PORTRAITS; k++) if (!used.has(k)) return k;
  return 0;
}
function uniqueName(room, name, exceptSlot) {
  const used = new Set(room.slots.filter((s, i) => s && i !== exceptSlot).map(s => s.name));
  let n = name, k = 2;
  while (used.has(n)) n = `${name} ${k++}`;
  return n;
}
function slotOf(room, id) { return room.slots.findIndex(s => s && !s.bot && s.id === id); }
function humans(room) { return room.slots.filter(s => s && !s.bot); }
function pickHost(room) {
  const h = humans(room).find(s => (clients.get(s.id) || {}).ws) || humans(room)[0];
  room.host = h ? h.id : null;
}

// ---------- партия ----------
function pushInput(room, from, d) {
  const g = room.game; if (!g) return;
  const msg = { t: 'input', seq: ++g.seq, from, d };
  g.log.push(msg);
  room.slots.forEach(s => { if (s && !s.bot) send(clients.get(s.id), msg); });
}
function dropSeat(room, id) {
  const g = room.game; if (!g) return;
  const seat = g.seatOf[id];
  if (seat == null || g.dropped[seat]) return;
  g.dropped[seat] = true;
  pushInput(room, -1, { a: 'drop', seat });
  // все игроки-люди ушли — партию не доигрываем компьютерами, она просто закрывается
  if (g.players.every((p, i) => !p.owner || g.dropped[i])) {
    room.game = null;
    if (!humans(room).some(s => (clients.get(s.id) || {}).ws)) rooms.delete(room.code);
  }
}
function startGame(room) {
  const list = room.slots.map((s, i) => s && { s, i }).filter(Boolean);
  const SEATS_FOR = { 2: [0, 2], 3: [0, 1, 2], 4: [0, 1, 2, 3] };
  const seats = SEATS_FOR[list.length];
  const players = list.map(({ s }, k) => ({ seat: seats[k], name: s.name, portrait: s.portrait, cpu: !!s.bot, owner: s.bot ? null : s.id }));
  const seatOf = {};
  players.forEach((p, idx) => { if (p.owner) seatOf[p.owner] = idx; });
  room.game = { id: rid(), seed: crypto.randomInt(1, 2 ** 31 - 1), seq: 0, log: [], players, seatOf, dropped: {}, nonces: new Set(), map: room.map, started: now() };
  room.slots.forEach(s => { if (s && !s.bot) s.ready = false; });
  const start = { t: 'start', gid: room.game.id, seed: room.game.seed, map: room.map, players };
  room.slots.forEach(s => { if (s && !s.bot) send(clients.get(s.id), start); });
  broadcastRoom(room);
}

// ---------- сообщения ----------
function leaveRoom(cl, quiet) {
  const room = rooms.get(cl.room); cl.room = null;
  if (!room) return;
  const i = slotOf(room, cl.id);
  if (room.game) dropSeat(room, cl.id);
  if (i >= 0) room.slots[i] = null;
  if (!humans(room).length) { rooms.delete(room.code); return; }
  if (room.host === cl.id) pickHost(room);
  if (!quiet) broadcastRoom(room);
}
function handle(cl, m) {
  const room = rooms.get(cl.room);
  switch (m.t) {
    case 'ping': send(cl, { t: 'pong' }); return;
    case 'create': {
      if (room) leaveRoom(cl);
      const code = newCode();
      const r = { code, host: cl.id, map: MAPS.includes(m.map) ? m.map : 'small', slots: [null, null, null, null], game: null, touched: now() };
      r.slots[0] = { id: cl.id, name: clean(m.name, 14) || 'Герой', portrait: 0, ready: false };
      r.slots[0].portrait = freePortrait(r, +m.portrait, 0);
      rooms.set(code, r); cl.room = code;
      broadcastRoom(r); return;
    }
    case 'join': {
      const code = String(m.code || '').replace(/\D/g, '').slice(0, 4);
      const r = rooms.get(code);
      if (!r) return send(cl, { t: 'error', code: 'noroom', msg: 'Комната с таким кодом не найдена.' });
      if (slotOf(r, cl.id) >= 0) { cl.room = code; broadcastRoom(r); return; }
      if (r.game) return send(cl, { t: 'error', code: 'playing', msg: 'В этой комнате уже идёт партия.' });
      const i = r.slots.findIndex(s => !s);
      if (i < 0) return send(cl, { t: 'error', code: 'full', msg: 'Комната заполнена: в ней уже 4 героя.' });
      if (room && room !== r) leaveRoom(cl);
      r.slots[i] = { id: cl.id, name: uniqueName(r, clean(m.name, 14) || 'Герой', i), portrait: freePortrait(r, +m.portrait, i), ready: false };
      cl.room = code; r.touched = now();
      broadcastRoom(r); return;
    }
    case 'leave': if (room) leaveRoom(cl); send(cl, { t: 'left' }); return;
  }
  if (!room) return send(cl, { t: 'error', code: 'noroom', msg: 'Ты не в комнате.' });
  room.touched = now();
  const me = slotOf(room, cl.id), isHost = room.host === cl.id;
  switch (m.t) {
    case 'ready': if (me >= 0 && !room.game) { room.slots[me].ready = !!m.v; broadcastRoom(room); } return;
    case 'portrait': if (me >= 0 && !room.game) { room.slots[me].portrait = freePortrait(room, +m.v, me); broadcastRoom(room); } return;
    case 'map': if (isHost && !room.game && MAPS.includes(m.v)) { room.map = m.v; broadcastRoom(room); } return;
    case 'bot': {
      if (!isHost || room.game) return;
      const i = +m.slot;
      if (m.add && i >= 0 && i < MAX_SEATS && !room.slots[i]) {
        const name = uniqueName(room, BOT_NAMES.find(n => !room.slots.some(s => s && s.name === n)) || 'Бот', i);
        room.slots[i] = { bot: true, name, portrait: freePortrait(room, 7 - i, i) };
      } else if (!m.add && room.slots[i] && room.slots[i].bot) room.slots[i] = null;
      broadcastRoom(room); return;
    }
    case 'start': {
      if (!isHost || room.game) return;
      // игроки без связи в лобби не держат старт: их место освобождается
      room.slots.forEach((s, i) => { if (s && !s.bot && s.id !== cl.id && !(clients.get(s.id) || {}).ws) room.slots[i] = null; });
      const hs = humans(room), all = room.slots.filter(Boolean);
      if (hs.length < 2) { broadcastRoom(room); return send(cl, { t: 'error', code: 'few', msg: 'Для игры по сети нужны хотя бы 2 игрока на связи. Против компьютера можно сыграть в режиме «Против ИИ».' }); }
      if (all.length < 2) return;
      if (hs.some(s => !s.ready && s.id !== cl.id)) return send(cl, { t: 'error', code: 'notready', msg: 'Не все игроки нажали «Готов».' });
      startGame(room); return;
    }
    case 'input': {
      const g = room.game; if (!g) return;
      const seat = g.seatOf[cl.id];
      if (seat == null || g.dropped[seat]) return;
      if (!m.d || typeof m.d !== 'object' || typeof m.d.a !== 'string' || m.d.a === 'drop') return;
      // повтор того же действия после переподключения — не дублировать
      if (m.d.n) { if (g.nonces.has(m.d.n)) return; g.nonces.add(m.d.n); }
      pushInput(room, seat, m.d); return;
    }
    case 'back': {   // партия закончилась — вернуться в комнату
      if (room.game) { room.game = null; }
      if (me >= 0) room.slots[me].ready = false;
      broadcastRoom(room); return;
    }
  }
}

// ---------- соединения ----------
const server = http.createServer((req, res) => {
  if (req.url === '/privacy' || req.url === '/privacy/' || req.url === '/privacy.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'public, max-age=3600' });
    res.end(PRIVACY);
    return;
  }
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' });
    res.end(`Логово дракона: сервер работает. Копия сервера: ${INSTANCE}. Комнат: ${rooms.size}, игроков онлайн: ${[...clients.values()].filter(c => c.ws).length}`);
    return;
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
wss.on('error', e => console.warn('wss error:', e && e.message));
server.on('clientError', (e, socket) => { try { socket.destroy(); } catch (x) { /* ignore */ } });
// последняя страховка: записать ошибку и продолжить работу, а не падать со всеми комнатами
process.on('uncaughtException', e => console.error('uncaught:', e));
process.on('unhandledRejection', e => console.error('unhandled:', e));

wss.on('connection', ws => {
  let cl = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  // ошибка одного соединения (обрыв, битый кадр) не должна ронять весь сервер
  ws.on('error', e => { console.warn('ws error:', e && e.code || e && e.message); try { ws.terminate(); } catch (x) { /* ignore */ } });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (!cl) {
      if (m.t !== 'hello') return;
      // переподключение по токену: тот же игрок, та же комната и партия
      const old = m.token && [...clients.values()].find(c => c.token === m.token);
      if (old) {
        cl = old;
        if (cl.ws && cl.ws !== ws) try { cl.ws.terminate(); } catch (e) { /* ignore */ }
        cl.ws = ws; clearTimeout(cl.dropTimer); cl.dropTimer = null;
      } else {
        cl = { id: rid(), token: rid() + rid(), ws, room: null };
        clients.set(cl.id, cl);
      }
      send(cl, { t: 'welcome', id: cl.id, token: cl.token, room: rooms.has(cl.room) ? cl.room : null });
      const room = rooms.get(cl.room);
      if (room) {
        send(cl, { t: 'room', room: roomView(room) });
        const g = room.game, seat = g && g.seatOf[cl.id];
        if (g && seat != null && !g.dropped[seat]) {
          // та же партия — дослать пропущенное; другая (старт прошёл мимо) — всю партию с начала
          const from = m.gid === g.id ? (+m.lastSeq || 0) : 0;
          send(cl, { t: 'resume', gid: g.id, seed: g.seed, map: g.map, players: g.players, seq: g.seq });
          g.log.filter(x => x.seq > from).forEach(x => send(cl, x));
        } else if (g && seat != null) send(cl, { t: 'gone' });
        broadcastRoom(room);
      }
      return;
    }
    try { handle(cl, m); } catch (e) { console.error(e); }
  });
  ws.on('close', () => {
    if (!cl || cl.ws !== ws) return;
    cl.ws = null;
    const room = rooms.get(cl.room);
    if (room) {
      broadcastRoom(room);
      cl.dropTimer = setTimeout(() => {
        if (cl.ws) return;
        const r = rooms.get(cl.room);
        if (r && r.game) { dropSeat(r, cl.id); broadcastRoom(r); }
        else if (r) leaveRoom(cl);
      }, room.game ? GRACE_MS : LOBBY_GRACE_MS);
    }
  });
});

// проверка «живых» соединений и уборка
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch (e) { /* ignore */ } });
  const t = now();
  for (const [code, r] of rooms) {
    const anyOnline = humans(r).some(s => (clients.get(s.id) || {}).ws);
    if (!anyOnline && t - r.touched > ROOM_IDLE_MS) rooms.delete(code);
  }
  for (const [id, c] of clients) if (!c.ws && !c.room && !c.dropTimer) clients.delete(id);
}, 25000);

server.listen(PORT, '0.0.0.0', () => console.log(`Dragon Lair server on 0.0.0.0:${PORT}`));
