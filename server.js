// ============================================================
//  server.js  —  IPC Debugger Backend
//  Express REST API + WebSocket real-time engine
//  Run: node server.js  (port 4000)
// ============================================================
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const cors      = require('cors');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static('../frontend/public'));

// ── In-memory IPC state ──────────────────────────────────────
const state = {
  pipes: {
    running: false,
    packetsSent: 0,
    packetsRecv: 0,
    bytesTotal:  0,
    history: []
  },
  mq: {
    queue:      [],   // [{ id, from, msg, priority, ts }]
    enqCount:   0,
    deqCount:   0,
    dropCount:  0,
    MAX:        8,
    history:    []
  },
  shm: {
    cells:      new Array(32).fill(null),   // null | { val, writer, state }
    writes:     0,
    reads:      0,
    conflicts:  0,
    mutexOn:    false,
    mutexOwner: null,
    counter:    0,
    history:    []
  }
};

let msgIdSeq = 0;

// ── WebSocket broadcast ──────────────────────────────────────
function broadcast(event, data) {
  const payload = JSON.stringify({ event, data, ts: Date.now() });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  });
}

function ipcLog(module, source, message, level = 'info') {
  const entry = { module, source, message, level, ts: Date.now() };
  broadcast('log', entry);
  return entry;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ════════════════════════════════════════════════════════════
//  REST API — PIPES
// ════════════════════════════════════════════════════════════

// GET  /api/pipes/status
app.get('/api/pipes/status', (req, res) => res.json(state.pipes));

// POST /api/pipes/start
app.post('/api/pipes/start', (req, res) => {
  if (state.pipes.running) return res.json({ ok: false, msg: 'Already running' });
  state.pipes.running = true;
  ipcLog('pipes', 'KERNEL', 'pipe() syscall — fd[0]=read, fd[1]=write created', 'ok');
  ipcLog('pipes', 'PROC-A', 'close(fd[0]) — write-only mode', 'info');
  ipcLog('pipes', 'PROC-B', 'close(fd[1]) — read-only mode', 'info');
  broadcast('pipes:started', state.pipes);
  res.json({ ok: true });
});

// POST /api/pipes/send   body: { data: string, speed: number }
app.post('/api/pipes/send', async (req, res) => {
  if (!state.pipes.running) return res.json({ ok: false, msg: 'Pipe not started' });
  const { data = 'HELLO', speed = 1200 } = req.body;

  res.json({ ok: true, data });

  // Simulate async pipe flow
  ipcLog('pipes', 'PROC-A', `write(fd[1], "${data}", ${data.length}) → buffering`, 'send');
  broadcast('pipes:packet', { stage: 'ab_start', data, progress: 0 });

  for (let i = 1; i <= 10; i++) {
    await sleep(speed / 10);
    broadcast('pipes:packet', { stage: 'ab_move', data, progress: i * 10 });
  }

  state.pipes.packetsSent++;
  state.pipes.bytesTotal += data.length;
  ipcLog('pipes', 'PROC-B', `read(fd[0]) → "${data}" (${data.length} bytes)`, 'recv');
  broadcast('pipes:packet', { stage: 'ab_done', data });

  await sleep(300);

  // B → C forward
  const fwd = `[FWD]${data}`;
  ipcLog('pipes', 'PROC-B', `write(fd2[1], "${fwd}") — forwarding downstream`, 'send');
  broadcast('pipes:packet', { stage: 'bc_start', data: fwd, progress: 0 });

  for (let i = 1; i <= 10; i++) {
    await sleep(speed / 10);
    broadcast('pipes:packet', { stage: 'bc_move', data: fwd, progress: i * 10 });
  }

  state.pipes.packetsRecv++;
  ipcLog('pipes', 'PROC-C', `read(fd2[0]) → "${fwd}" ✓`, 'ok');
  broadcast('pipes:packet', { stage: 'bc_done', data: fwd });
  broadcast('pipes:stats', { ...state.pipes });
});

// POST /api/pipes/reset
app.post('/api/pipes/reset', (req, res) => {
  state.pipes = { running: false, packetsSent: 0, packetsRecv: 0, bytesTotal: 0, history: [] };
  broadcast('pipes:reset', state.pipes);
  ipcLog('pipes', 'KERNEL', 'Pipes closed and reset', 'info');
  res.json({ ok: true });
});

// ════════════════════════════════════════════════════════════
//  REST API — MESSAGE QUEUE
// ════════════════════════════════════════════════════════════

app.get('/api/mq/status', (req, res) => res.json({
  queue: state.mq.queue, enqCount: state.mq.enqCount,
  deqCount: state.mq.deqCount, dropCount: state.mq.dropCount
}));

// POST /api/mq/enqueue   body: { from, msg, priority }
app.post('/api/mq/enqueue', (req, res) => {
  const { from = 'PROC-A', msg = 'MSG', priority = 'NORMAL' } = req.body;
  if (state.mq.queue.length >= state.mq.MAX) {
    state.mq.dropCount++;
    ipcLog('mq', from, `msgsnd() EAGAIN — queue full, "${msg}" dropped`, 'error');
    broadcast('mq:update', { ...state.mq });
    return res.json({ ok: false, reason: 'QUEUE_FULL' });
  }
  const item = { id: ++msgIdSeq, from, msg, priority, ts: Date.now() };
  state.mq.queue.push(item);
  state.mq.enqCount++;
  ipcLog('mq', from, `msgsnd("${msg}", priority=${priority}) → depth=${state.mq.queue.length}`, 'send');
  broadcast('mq:update', { queue: state.mq.queue, enqCount: state.mq.enqCount, deqCount: state.mq.deqCount, dropCount: state.mq.dropCount });
  res.json({ ok: true, item });
});

// POST /api/mq/dequeue   body: { receiver }
app.post('/api/mq/dequeue', (req, res) => {
  const { receiver = 'PROC-C' } = req.body;
  if (state.mq.queue.length === 0) {
    ipcLog('mq', receiver, 'msgrcv() ENOMSG — queue empty', 'warn');
    return res.json({ ok: false, reason: 'QUEUE_EMPTY' });
  }
  const item = state.mq.queue.shift();
  state.mq.deqCount++;
  ipcLog('mq', receiver, `msgrcv() → "${item.msg}" from ${item.from} (id=${item.id}) ✓`, 'recv');
  broadcast('mq:update', { queue: state.mq.queue, enqCount: state.mq.enqCount, deqCount: state.mq.deqCount, dropCount: state.mq.dropCount });
  res.json({ ok: true, item });
});

// POST /api/mq/reset
app.post('/api/mq/reset', (req, res) => {
  state.mq.queue = []; state.mq.enqCount = 0; state.mq.deqCount = 0; state.mq.dropCount = 0;
  broadcast('mq:update', { queue: [], enqCount: 0, deqCount: 0, dropCount: 0 });
  ipcLog('mq', 'KERNEL', 'Message queue flushed and reset', 'info');
  res.json({ ok: true });
});

// POST /api/mq/autorun
app.post('/api/mq/autorun', async (req, res) => {
  res.json({ ok: true });
  const msgs = ['PING','TASK','ALERT','DATA','HEARTBEAT','CMD','RESULT','QUERY'];
  const procs = ['PROC-A','PROC-B','PROC-C'];
  const prios = ['HIGH','NORMAL','LOW'];
  for (let i = 0; i < 12; i++) {
    await sleep(600);
    if (Math.random() < 0.65) {
      const from = procs[i % 3], msg = msgs[i % msgs.length], priority = prios[i % 3];
      if (state.mq.queue.length < state.mq.MAX) {
        const item = { id: ++msgIdSeq, from, msg, priority, ts: Date.now() };
        state.mq.queue.push(item); state.mq.enqCount++;
        ipcLog('mq', from, `AUTO msgsnd("${msg}", priority=${priority})`, 'send');
        broadcast('mq:update', { queue: state.mq.queue, enqCount: state.mq.enqCount, deqCount: state.mq.deqCount, dropCount: state.mq.dropCount });
      }
    } else if (state.mq.queue.length > 0) {
      const item = state.mq.queue.shift(); state.mq.deqCount++;
      ipcLog('mq', 'PROC-C', `AUTO msgrcv() → "${item.msg}" ✓`, 'recv');
      broadcast('mq:update', { queue: state.mq.queue, enqCount: state.mq.enqCount, deqCount: state.mq.deqCount, dropCount: state.mq.dropCount });
    }
  }
  ipcLog('mq', 'KERNEL', 'Auto-run complete', 'ok');
});

// ════════════════════════════════════════════════════════════
//  REST API — SHARED MEMORY
// ════════════════════════════════════════════════════════════

app.get('/api/shm/status', (req, res) => res.json(state.shm));

// POST /api/shm/write   body: { proc, idx, value }
app.post('/api/shm/write', async (req, res) => {
  const { proc = 'PROC-A', idx = 0, value = 42 } = req.body;
  const addr = `0x${(idx * 4).toString(16).padStart(2, '0')}`;

  if (state.shm.mutexOn) {
    if (state.shm.mutexOwner && state.shm.mutexOwner !== proc) {
      ipcLog('shm', proc, `mutex_lock() → BLOCKED (owned by ${state.shm.mutexOwner})`, 'warn');
      broadcast('shm:blocked', { proc });
      return res.json({ ok: false, reason: 'MUTEX_LOCKED', owner: state.shm.mutexOwner });
    }
    state.shm.mutexOwner = proc;
    ipcLog('shm', proc, `mutex_lock() → acquired`, 'ok');
    broadcast('shm:lock', { owner: proc });
  }

  ipcLog('shm', proc, `shm_write(addr=${addr}, val=0x${value.toString(16)}) →`, 'send');
  broadcast('shm:writing', { proc, idx, value });

  await sleep(400);

  // detect conflict
  if (state.shm.cells[idx] && state.shm.cells[idx].writer !== proc) {
    state.shm.conflicts++;
    ipcLog('shm', proc, `CONFLICT at ${addr} — overwriting ${state.shm.cells[idx].writer}'s data!`, 'error');
    broadcast('shm:conflict', { idx, proc, prev: state.shm.cells[idx].writer });
  }

  state.shm.cells[idx] = { val: `0x${value.toString(16).toUpperCase().padStart(2,'0')}`, writer: proc, state: 'written' };
  state.shm.writes++;
  ipcLog('shm', proc, `wrote 0x${value.toString(16).toUpperCase()} → ${addr} ✓`, 'ok');
  broadcast('shm:update', { cells: state.shm.cells, writes: state.shm.writes, conflicts: state.shm.conflicts });

  if (state.shm.mutexOn) {
    state.shm.mutexOwner = null;
    ipcLog('shm', proc, `mutex_unlock()`, 'info');
    broadcast('shm:unlock', {});
  }
  res.json({ ok: true });
});

// POST /api/shm/read
app.post('/api/shm/read', (req, res) => {
  let cnt = 0;
  state.shm.cells.forEach((c, i) => { if (c) { state.shm.cells[i].state = 'reading'; cnt++; } });
  state.shm.reads += cnt;
  ipcLog('shm', 'PROC-C', `shmat() read ${cnt} cells ✓`, 'recv');
  broadcast('shm:update', { cells: state.shm.cells, reads: state.shm.reads, conflicts: state.shm.conflicts });
  res.json({ ok: true, read: cnt });
});

// POST /api/shm/race
app.post('/api/shm/race', async (req, res) => {
  res.json({ ok: true });
  if (state.shm.mutexOn) {
    ipcLog('shm', 'KERNEL', 'Mutex ON — race condition PREVENTED ✓', 'ok');
    broadcast('shm:race_prevented', {});
    return;
  }
  const idx = Math.floor(Math.random() * 20);
  const addr = `0x${(idx * 4).toString(16).padStart(2, '0')}`;
  ipcLog('shm', 'KERNEL', `⚡ RACE: PROC-A and PROC-B both write to ${addr} simultaneously!`, 'error');
  broadcast('shm:race_start', { idx });

  // Both "start" at same time
  ipcLog('shm', 'PROC-A', `read ${addr} → 0x00 (stale copy)`, 'warn');
  ipcLog('shm', 'PROC-B', `read ${addr} → 0x00 (stale copy)`, 'warn');
  await sleep(300);
  broadcast('shm:writing', { proc: 'PROC-A', idx, value: 0xAA });
  broadcast('shm:writing', { proc: 'PROC-B', idx, value: 0xBB });
  await sleep(350);
  // B overwrites A
  state.shm.cells[idx] = { val: '0xAA', writer: 'PROC-A', state: 'conflict' };
  broadcast('shm:update', { cells: state.shm.cells, writes: state.shm.writes, conflicts: state.shm.conflicts });
  await sleep(200);
  state.shm.cells[idx] = { val: '0xBB', writer: 'PROC-B', state: 'conflict' };
  state.shm.conflicts++;
  ipcLog('shm', 'RESULT', `DATA CORRUPTION: ${addr} expected 0xAA (PROC-A) got 0xBB (PROC-B)!`, 'error');
  broadcast('shm:race_done', { idx, conflicts: state.shm.conflicts });
  broadcast('shm:update', { cells: state.shm.cells, writes: state.shm.writes, conflicts: state.shm.conflicts });
});

// POST /api/shm/mutex
app.post('/api/shm/mutex', (req, res) => {
  state.shm.mutexOn = !state.shm.mutexOn;
  ipcLog('shm', 'KERNEL', `Mutex ${state.shm.mutexOn ? 'ENABLED' : 'DISABLED'}`, state.shm.mutexOn ? 'ok' : 'warn');
  broadcast('shm:mutex', { on: state.shm.mutexOn });
  res.json({ ok: true, mutexOn: state.shm.mutexOn });
});

// POST /api/shm/reset
app.post('/api/shm/reset', (req, res) => {
  state.shm.cells = new Array(32).fill(null);
  state.shm.writes = 0; state.shm.reads = 0; state.shm.conflicts = 0;
  state.shm.mutexOwner = null; state.shm.counter = 0;
  broadcast('shm:update', { cells: state.shm.cells, writes: 0, reads: 0, conflicts: 0 });
  broadcast('shm:unlock', {});
  ipcLog('shm', 'KERNEL', 'Shared memory segment cleared', 'info');
  res.json({ ok: true });
});

// ── WebSocket connection handler ─────────────────────────────
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ event: 'connected', data: { pid: process.pid, ts: Date.now() } }));
  ipcLog('system', 'KERNEL', `Client connected — ${wss.clients.size} active`, 'ok');
  ws.on('close', () => ipcLog('system', 'KERNEL', `Client disconnected`, 'info'));
});

// ── Start server ─────────────────────────────────────────────
const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`\n  IPC Debugger Backend running on http://localhost:${PORT}`);
  console.log(`  WebSocket on  ws://localhost:${PORT}`);
  console.log(`  Frontend:     open frontend/public/index.html\n`);
});
