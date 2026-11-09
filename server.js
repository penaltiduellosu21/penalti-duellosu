// Penaltı Düellosu — sunucu
// Hem oyun sayfasını yayınlar hem de TikTok canlı yayınındaki yorum / hediye /
// beğeni / takipleri yakalayıp oyuna iletir.
//
// Bulutta (Render vb.):  KULLANICI ortam değişkenine TikTok kullanıcı adını yaz.
// Bilgisayarda:          node server.js tiktok_kullanici_adin   → http://localhost:8080

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebcastPushConnection } = require('tiktok-live-connector');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const clean = u => String(u || '').trim().replace(/^@/, '').toLowerCase().replace(/[^a-z0-9._]/g, '');
// Sabit kullanıcı adı verilirse sunucu SADECE o hesaba bağlanır (başkaları kullanamaz).
const FIXED_USER = clean(process.argv[2] || process.env.KULLANICI || process.env.TIKTOK_USER);

/* ---------------- oyun sayfasını yayınla ---------------- */
const ROOT = __dirname;
const TYPES = { '.html': 'text/html; charset=utf-8', '.mp3': 'audio/mpeg' };

const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch { p = '/'; }
  if (p === '/ping') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (p === '/' || p === '/index.html') p = '/oyun.html';
  const ext = path.extname(p);
  const ok = p === '/oyun.html' || (/^\/sesler\/[a-z0-9_-]+\.mp3$/i.test(p));
  if (!ok) { res.writeHead(404); return res.end('Bulunamadı'); }
  fs.readFile(path.join(ROOT, p), (err, data) => {
    if (err) { res.writeHead(404); return res.end('Bulunamadı'); }
    res.writeHead(200, { 'content-type': TYPES[ext], 'cache-control': 'no-cache' });
    res.end(data);
  });
});

/* ---------------- TikTok bağlantıları ---------------- */
const rooms = new Map();   // kullanıcı adı -> oda

function broadcast(room, obj) {
  const msg = JSON.stringify(obj);
  for (const c of room.clients) if (c.readyState === 1) c.send(msg);
}
function setState(room, state, msg) {
  room.state = state; room.msg = msg;
  console.log(`[@${room.user}] ${msg}`);
  broadcast(room, { type: 'status', state, msg });
}
function schedule(room, ms) {
  clearTimeout(room.retry);
  room.retry = setTimeout(() => tryConnect(room), ms);
}

function tryConnect(room) {
  clearTimeout(room.retry);
  if (!room.active) return;
  if (room.conn) { room.conn.removeAllListeners(); try { room.conn.disconnect(); } catch {} }

  const conn = new WebcastPushConnection(room.user);
  room.conn = conn;
  const live = () => room.conn === conn;

  conn.on('chat', d => live() && broadcast(room, { type: 'chat', user: d.uniqueId, nick: d.nickname, text: d.comment }));
  conn.on('gift', d => {
    if (!live()) return;
    if (d.giftType === 1 && !d.repeatEnd) return;           // seri hediyede sadece seri bitince say
    broadcast(room, { type: 'gift', user: d.uniqueId, nick: d.nickname, gift: d.giftName,
                      coins: d.diamondCount || 1, count: d.repeatCount || 1 });
  });
  conn.on('like', d => live() && broadcast(room, { type: 'like', user: d.uniqueId, nick: d.nickname, count: d.likeCount || 1 }));
  conn.on('follow', d => live() && broadcast(room, { type: 'follow', user: d.uniqueId, nick: d.nickname }));
  conn.on('streamEnd', () => { if (!live()) return; setState(room, 'error', 'Yayın bitti'); schedule(room, 20000); });
  conn.on('disconnected', () => { if (!live() || !room.active) return; setState(room, 'connecting', 'Bağlantı koptu, yeniden deneniyor'); schedule(room, 10000); });

  setState(room, 'connecting', `@${room.user} yayınına bağlanılıyor`);
  conn.connect()
    .then(() => live() && setState(room, 'connected', `@${room.user} yayınına bağlandı`))
    .catch(err => {
      if (!live()) return;
      const why = (err && err.message ? err.message : String(err)).slice(0, 140);
      setState(room, 'error', `Yayın bulunamadı (açık mı?) — 20 sn sonra tekrar. [${why}]`);
      schedule(room, 20000);
    });
}

function getRoom(user) {
  let room = rooms.get(user);
  if (!room) {
    room = { user, clients: new Set(), state: 'connecting', msg: '', conn: null, retry: null, idle: null, active: false };
    rooms.set(user, room);
  }
  return room;
}
function stopRoom(room) {
  room.active = false; clearTimeout(room.retry);
  if (room.conn) { room.conn.removeAllListeners(); try { room.conn.disconnect(); } catch {} }
  rooms.delete(room.user);
  console.log(`[@${room.user}] ekran kalmadı, bağlantı kapatıldı`);
}

/* ---------------- oyun ekranları ---------------- */
const wss = new WebSocketServer({ server });
wss.on('connection', (ws, req) => {
  let asked = '';
  try { asked = clean(new URL(req.url, 'http://x').searchParams.get('kullanici')); } catch {}
  const user = FIXED_USER || asked;
  if (!user) {
    ws.send(JSON.stringify({ type: 'status', state: 'error', msg: 'TikTok kullanıcı adı ayarlanmamış (KULLANICI)' }));
    return ws.close();
  }
  const room = getRoom(user);
  room.clients.add(ws);
  clearTimeout(room.idle);
  ws.send(JSON.stringify({ type: 'status', state: room.state, msg: room.msg || 'Bağlanılıyor' }));
  if (!room.active) { room.active = true; tryConnect(room); }

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('close', () => {
    room.clients.delete(ws);
    if (!room.clients.size) room.idle = setTimeout(() => stopRoom(room), 60000);
  });
});
// kopuk bağlantıları temizle + bağlantıyı canlı tut
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Penaltı Düellosu çalışıyor → http://localhost:${PORT}`);
  console.log(FIXED_USER ? `TikTok hesabı: @${FIXED_USER}` : 'Kullanıcı adı adresten alınacak: ?kullanici=...');
});
