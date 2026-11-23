// Zero-dependency server: serves the game page and relays WebSocket messages between the two players of a room.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map();   // code -> { host, guest }

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
function send(peer, opcode, payload) { try { if (peer && !peer.socket.destroyed) peer.socket.write(frame(opcode, payload)); } catch (e) {} }
function sendJson(peer, obj) { send(peer, 1, Buffer.from(JSON.stringify(obj))); }

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  if (url.pathname !== '/ws' || !key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);

  const code = (url.searchParams.get('room') || '').slice(0, 16);
  const peer = { socket, room: null, role: null, buf: Buffer.alloc(0), alive: true };
  let room = rooms.get(code);
  if (!room) { room = { host: null, guest: null }; rooms.set(code, room); }
  if (!room.host) { room.host = peer; peer.role = 'host'; }
  else if (!room.guest) { room.guest = peer; peer.role = 'guest'; }
  else { sendJson(peer, { t: 'full' }); socket.end(); return; }
  peer.room = room;
  sendJson(peer, { t: 'role', r: peer.role });
  if (room.host && room.guest) { sendJson(room.host, { t: 'ready', r: 'host' }); sendJson(room.guest, { t: 'ready', r: 'guest' }); }

  const other = () => (peer.role === 'host' ? room.guest : room.host);
  function leave() {
    if (!peer.room) return;
    const o = other();
    if (peer.role === 'host') { room.host = null; if (room.guest) { sendJson(room.guest, { t: 'left' }); room.guest.socket.end(); room.guest = null; } rooms.delete(code); }
    else { room.guest = null; if (o) sendJson(o, { t: 'left' }); if (!room.host) rooms.delete(code); }
    peer.room = null;
  }
  socket.on('data', (chunk) => {
    peer.buf = Buffer.concat([peer.buf, chunk]);
    while (peer.buf.length >= 2) {
      const b = peer.buf;
      const opcode = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { socket.destroy(); return; }
      const need = off + (masked ? 4 : 0) + len;
      if (b.length < need) return;
      let payload = b.slice(off + (masked ? 4 : 0), need);
      if (masked) { const m = b.slice(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      peer.buf = b.slice(need);
      if (opcode === 8) { socket.end(frame(8, Buffer.alloc(0))); return; }
      if (opcode === 9) { send(peer, 10, payload); continue; }
      if (opcode === 10) { peer.alive = true; continue; }
      if (opcode === 1 || opcode === 2) { const o = other(); if (o) send(o, opcode, payload); }
    }
  });
  socket.on('close', leave);
  socket.on('error', () => { try { socket.destroy(); } catch (e) {} });
  const ping = setInterval(() => { if (socket.destroyed) { clearInterval(ping); return; } send(peer, 9, Buffer.alloc(0)); }, 25000);
  socket.on('close', () => clearInterval(ping));
});

server.listen(PORT, () => console.log('Soccer server on port ' + PORT));
