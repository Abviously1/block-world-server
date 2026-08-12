// ══════════════════════════════════════════════════════════════════
// BLOCK WORLD — Multiplayer Server
// Handles rooms, 7-digit join codes, lobbies, and real-time sync of
// player movement, block edits, and chat for the Block World client.
// ══════════════════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

const MAX_PLAYERS = 10;
const COUNTDOWN_STEP_MS = 900;
const STALE_ROOM_MS = 6 * 60 * 60 * 1000; // 6 hours

// code (string) -> room
// room = { code, hostId, seed, started, players: Map(id -> {id,name,outfit,ready,score}), createdAt }
const rooms = new Map();

app.get('/', (req, res) => {
  res.send(`Block World multiplayer server is running. Active rooms: ${rooms.size}`);
});

function sanitizeName(name) {
  if (typeof name !== 'string') return '';
  return name.trim().slice(0, 20).replace(/[<>]/g, '') || '';
}

function generateRoomCode() {
  let code;
  do {
    code = String(Math.floor(1000000 + Math.random() * 9000000)); // 7 digits, no leading zero
  } while (rooms.has(code));
  return code;
}

function getMyRoom(socket) {
  const code = socket.data.roomCode;
  if (!code) return null;
  return rooms.get(code) || null;
}

function emitLobbyUpdate(code) {
  const room = rooms.get(code);
  if (!room) return;
  io.to(code).emit('lobbyUpdate', {
    hostId: room.hostId,
    players: [...room.players.values()]
  });
}

function playersPublicList(room) {
  return [...room.players.values()].map(p => ({ id: p.id, name: p.name, outfit: p.outfit }));
}

function destroyRoom(code, reason) {
  const room = rooms.get(code);
  if (!room) return;
  io.to(code).emit('gameEnded', { reason });
  const roomSockets = io.sockets.adapter.rooms.get(code);
  if (roomSockets) {
    for (const id of roomSockets) {
      const s = io.sockets.sockets.get(id);
      if (s) {
        s.leave(code);
        s.data.roomCode = null;
      }
    }
  }
  rooms.delete(code);
}

function handleLeave(socket) {
  const code = socket.data.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  socket.data.roomCode = null;
  if (!room) return;

  if (room.hostId === socket.id) {
    // Host leaving ends the game for everyone (by design — see product spec).
    destroyRoom(code, 'host_left');
    return;
  }

  const name = (room.players.get(socket.id) || {}).name || 'A player';
  room.players.delete(socket.id);
  socket.leave(code);
  io.to(code).emit('playerLeft', { id: socket.id, name });
  emitLobbyUpdate(code);
  if (room.players.size === 0) rooms.delete(code);
}

io.on('connection', (socket) => {
  socket.data.roomCode = null;
  socket.data.name = null;

  // ── Host a new room ──────────────────────────────────────────
  socket.on('hostGame', ({ name, outfit } = {}) => {
    name = sanitizeName(name);
    if (!name) return socket.emit('hostError', 'Please enter a valid name.');

    const code = generateRoomCode();
    const room = {
      code,
      hostId: socket.id,
      seed: Math.floor(Math.random() * 1000000),
      started: false,
      allowLateJoins: false,
      epochMs: null,
      players: new Map(),
      createdAt: Date.now()
    };
    room.players.set(socket.id, { id: socket.id, name, outfit: outfit || {}, ready: false, score: 0 });
    rooms.set(code, room);

    socket.join(code);
    socket.data.roomCode = code;
    socket.data.name = name;

    socket.emit('hostSuccess', { code });
    emitLobbyUpdate(code);
  });

  // ── Join an existing room ───────────────────────────────────
  socket.on('joinGame', ({ code, name, outfit } = {}) => {
    name = sanitizeName(name);
    code = String(code || '').trim();
    const room = rooms.get(code);

    if (!room) return socket.emit('joinError', 'Room not found. Check the code and try again.');
    if (!name) return socket.emit('joinError', 'Please enter a valid name.');
    if (room.players.size >= MAX_PLAYERS) return socket.emit('joinError', `Room is full (${MAX_PLAYERS}/${MAX_PLAYERS} players).`);

    if (room.started && !room.allowLateJoins) {
      return socket.emit('joinError', 'This game has already started.');
    }

    room.players.set(socket.id, { id: socket.id, name, outfit: outfit || {}, ready: false, score: 0 });
    socket.join(code);
    socket.data.roomCode = code;
    socket.data.name = name;

    if (room.started) {
      // Late join: drop straight into the running game — no lobby, no countdown.
      socket.emit('joinMidGame', { code, seed: room.seed, epochMs: room.epochMs, players: playersPublicList(room) });
      socket.to(code).emit('playerJoined', { id: socket.id, name, outfit: outfit || {} });
    } else {
      socket.emit('joinSuccess', { code });
      emitLobbyUpdate(code);
    }
  });

  socket.on('setAllowLateJoins', (allow) => {
    const room = getMyRoom(socket);
    if (!room || room.hostId !== socket.id) return;
    room.allowLateJoins = !!allow;
  });

  // ── Lobby updates ───────────────────────────────────────────
  socket.on('outfitUpdate', (outfit) => {
    const room = getMyRoom(socket);
    if (!room) return;
    const p = room.players.get(socket.id);
    if (!p) return;
    p.outfit = outfit || {};
    emitLobbyUpdate(room.code);
  });

  socket.on('setReady', (ready) => {
    const room = getMyRoom(socket);
    if (!room) return;
    const p = room.players.get(socket.id);
    if (!p) return;
    p.ready = !!ready;
    emitLobbyUpdate(room.code);
  });

  socket.on('flappyScore', (score) => {
    const room = getMyRoom(socket);
    if (!room) return;
    const p = room.players.get(socket.id);
    if (!p) return;
    score = Math.max(0, Math.min(999999, score | 0));
    if (score > p.score) {
      p.score = score;
      emitLobbyUpdate(room.code);
      io.to(room.code).emit('flappyScoreUpdate', { name: p.name, score: p.score });
    }
  });

  // ── Start game (host only) ──────────────────────────────────
  socket.on('startGame', () => {
    const room = getMyRoom(socket);
    if (!room || room.hostId !== socket.id || room.started) return;
    room.started = true;

    let n = 3;
    io.to(room.code).emit('countdown', n);
    const iv = setInterval(() => {
      n -= 1;
      if (!rooms.has(room.code)) { clearInterval(iv); return; } // room ended mid-countdown
      if (n >= 0) {
        io.to(room.code).emit('countdown', n);
      } else {
        clearInterval(iv);
        room.epochMs = Date.now();
        io.to(room.code).emit('gameStart', { players: playersPublicList(room), seed: room.seed, epochMs: room.epochMs });
      }
    }, COUNTDOWN_STEP_MS);
  });

  // Late-joining sync: client asks for the full player list once its world has loaded.
  socket.on('requestLobbyState', () => {
    const room = getMyRoom(socket);
    if (!room) return;
    socket.emit('allPlayers', { players: playersPublicList(room), myId: socket.id });
  });

  // ── In-game sync ─────────────────────────────────────────────
  socket.on('playerMove', (data) => {
    const room = getMyRoom(socket);
    if (!room || !room.started || !data) return;
    socket.to(room.code).emit('playerMoved', {
      id: socket.id,
      x: data.x, y: data.y, z: data.z, yaw: data.yaw
    });
  });

  socket.on('blockChange', (data) => {
    const room = getMyRoom(socket);
    if (!room || !data) return;
    socket.to(room.code).emit('blockChanged', {
      x: data.x, y: data.y, z: data.z, blockId: data.blockId
    });
  });

  socket.on('chatMessage', (text) => {
    const room = getMyRoom(socket);
    if (!room) return;
    const p = room.players.get(socket.id);
    if (!p) return;
    text = String(text || '').slice(0, 120).trim();
    if (!text) return;
    io.to(room.code).emit('chatMessage', { name: p.name, text });
  });

  // ── Ending / leaving ─────────────────────────────────────────
  socket.on('endGame', () => {
    const room = getMyRoom(socket);
    if (!room || room.hostId !== socket.id) return;
    destroyRoom(room.code, 'host_ended');
  });

  socket.on('leaveGame', () => handleLeave(socket));
  socket.on('disconnect', () => handleLeave(socket));
});

// Sweep out abandoned rooms every 30 minutes (safety net, e.g. crashed clients).
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.players.size === 0 || now - room.createdAt > STALE_ROOM_MS) {
      rooms.delete(code);
    }
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Block World server listening on port ${PORT}`));
