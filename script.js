// ============================================================
//  script.js — IPC Debugger Frontend
//  Connects to Express backend via REST + WebSocket.
//  Falls back to offline simulation if backend is unavailable.
// ============================================================

const BASE_URL = 'http://localhost:4000';
const WS_URL   = 'ws://localhost:4000';

// ── State ─────────────────────────────────────────────────────
let ws = null;
let wsConnected = false;
let logPaused   = false;
let logEntries  = 0;
let offlineMode = false;   // fallback when no backend

// ── Clock ─────────────────────────────────────────────────────
setInterval(() => {
  const t = new Date();
  const pad = n => String(n).padStart(2, '0');
  document.getElementById('sys-clock').textContent =
    `${pad(t.getHours())}:${pad(t.getMinutes())}:${pad(t.getSeconds())}`;
}, 1000);

// ── Tabs ──────────────────────────────────────────────────────
document.querySelectorAll('.nav-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
  });
});

// ── Log helpers ───────────────────────────────────────────────
function addLog(source, message, level = 'info') {
  if (logPaused) return;
  const box = document.getElementById('log-box');
  const t = new Date();
  const ts = `${String(t.getHours()).padStart(2,'0')}:${String(t.getMinutes()).padStart(2,'0')}:${String(t.getSeconds()).padStart(2,'0')}`;
  const el = document.createElement('div');
  el.className = `log-entry ${level}`;
  el.innerHTML = `<span class="log-ts">${ts}</span><span class="log-src">[${source}]</span>${message}`;
  box.appendChild(el);
  box.scrollTop = box.scrollHeight;
  logEntries++;
  if (logEntries > 300) { box.firstChild && box.removeChild(box.firstChild); }
  document.getElementById('log-count').textContent = `${logEntries} entries`;
}
function clearLog() { document.getElementById('log-box').innerHTML = ''; logEntries = 0; }
function toggleLogPause() {
  logPaused = !logPaused;
  document.getElementById('btn-pause-log').textContent = logPaused ? '▶' : '⏸';
}

// ── WebSocket setup ───────────────────────────────────────────
function initWS() {
  try {
    ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      wsConnected = true; offlineMode = false;
      document.getElementById('conn-dot').className = 'conn-dot connected';
      document.getElementById('conn-label').textContent = 'CONNECTED';
      document.getElementById('ws-status').textContent = 'ws: live';
      addLog('SYSTEM', 'WebSocket connected to backend', 'ok');
    };

    ws.onmessage = (e) => {
      try {
        const { event, data } = JSON.parse(e.data);
        handleWSEvent(event, data);
      } catch (_) {}
    };

    ws.onerror = () => activateOfflineMode();
    ws.onclose = () => {
      wsConnected = false;
      document.getElementById('conn-dot').className = 'conn-dot error';
      document.getElementById('conn-label').textContent = 'OFFLINE';
      document.getElementById('ws-status').textContent = 'ws: —';
      if (!offlineMode) activateOfflineMode();
    };
  } catch (_) { activateOfflineMode(); }
}

function activateOfflineMode() {
  if (offlineMode) return;
  offlineMode = true;
  document.getElementById('conn-dot').className = 'conn-dot';
  document.getElementById('conn-label').textContent = 'DEMO MODE';
  document.getElementById('ws-status').textContent = 'ws: sim';
  document.getElementById('sys-pid').textContent = String(Math.floor(Math.random() * 9000) + 1000);
  addLog('SYSTEM', 'Backend unavailable — running in offline simulation mode', 'warn');
  addLog('SYSTEM', 'Start backend: cd backend && npm install && node server.js', 'info');
}

// ── WebSocket event router ────────────────────────────────────
function handleWSEvent(event, data) {
  switch (event) {
    case 'connected':
      document.getElementById('sys-pid').textContent = String(data.pid);
      break;
    case 'log':
      addLog(data.source, data.message, data.level);
      break;
    case 'pipes:started':
      setPipesStarted();
      break;
    case 'pipes:packet':
      handlePipePacket(data);
      break;
    case 'pipes:stats':
      updatePipeStats(data);
      break;
    case 'pipes:reset':
      resetPipeUI();
      break;
    case 'mq:update':
      renderQueue(data);
      break;
    case 'shm:update':
      renderSHM(data);
      break;
    case 'shm:writing':
      shmWritingUI(data.proc);
      break;
    case 'shm:race_start':
      document.getElementById('race-banner').className = 'race-banner show';
      document.getElementById('race-text').textContent = `RACE CONDITION at 0x${(data.idx*4).toString(16).padStart(2,'0')} — concurrent write!`;
      break;
    case 'shm:race_done':
      updateSHMStats(data);
      break;
    case 'shm:race_prevented':
      const rb = document.getElementById('race-banner');
      rb.className = 'race-banner show ok';
      document.getElementById('race-text').textContent = 'MUTEX ACTIVE — race condition prevented ✓';
      setTimeout(() => rb.className = 'race-banner', 3000);
      break;
    case 'shm:conflict':
      addLog('KERNEL', `CONFLICT at 0x${(data.idx*4).toString(16)} — ${data.proc} overwrites ${data.prev}`, 'error');
      break;
    case 'shm:lock':
      setLock(true, data.owner);
      break;
    case 'shm:unlock':
      setLock(false);
      break;
    case 'shm:blocked':
      setBox(`shm-pbox-${data.proc.slice(-1).toLowerCase()}`, 'error');
      setBadge(`shm-pbadge-${data.proc.slice(-1).toLowerCase()}`, 'BLOCKED', 'error');
      setTimeout(() => {
        setBox(`shm-pbox-${data.proc.slice(-1).toLowerCase()}`, '');
        setBadge(`shm-pbadge-${data.proc.slice(-1).toLowerCase()}`, 'IDLE', 'idle');
      }, 1000);
      break;
    case 'shm:mutex':
      updateMutexUI(data.on);
      break;
  }
}

// ── REST helper ───────────────────────────────────────────────
async function post(path, body = {}) {
  if (offlineMode) return null;
  try {
    const r = await fetch(BASE_URL + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    return r.ok ? r.json() : null;
  } catch (_) { return null; }
}

// ── UI helpers ────────────────────────────────────────────────
function setBox(id, cls) {
  const el = document.getElementById(id);
  if (el) el.className = `proc-box ${cls}`;
}
function setBadge(id, text, cls) {
  const el = document.getElementById(id);
  if (el) { el.className = `proc-badge ${cls}`; el.textContent = text; }
}
function setStat(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ════════════════════════════════════════════════════════════
//  PIPES
// ════════════════════════════════════════════════════════════
let pipeRunning = false, pipeSent = 0, pipeRecv = 0, pipeBytes = 0;

function setPipesStarted() {
  pipeRunning = true;
  document.getElementById('btn-pipe-start').disabled = true;
  document.getElementById('btn-pipe-send').disabled = false;
  ['pbox-a1','pbox-b1','pbox-b2','pbox-c1'].forEach(id => setBox(id, 'active'));
  ['pbadge-a1','pbadge-b1','pbadge-b2','pbadge-c1'].forEach((id, i) =>
    setBadge(id, i < 3 ? 'READY' : 'LISTEN', i < 3 ? 'sending' : 'receiving'));
  setStat('p-stat-status', 'READY');
}

function updatePipeStats(d) {
  setStat('p-stat-sent',  d.packetsSent);
  setStat('p-stat-recv',  d.packetsRecv);
  setStat('p-stat-bytes', d.bytesTotal);
}

async function handlePipePacket(d) {
  const { stage, data, progress } = d;
  switch (stage) {
    case 'ab_start':
      setBadge('pbadge-a1', 'WRITING', 'sending');
      setBox('pbox-a1', 'active');
      const pktAB = document.getElementById('pkt-ab');
      pktAB.textContent = data.substring(0, 10);
      pktAB.style.setProperty('--fly-dur', document.getElementById('pipe-speed').value / 1000 + 's');
      break;
    case 'ab_move':
      document.getElementById('pfill-ab').style.width = progress + '%';
      if (progress === 50) {
        const pk = document.getElementById('pkt-ab');
        pk.className = 'pipe-pkt fly';
        setBox('pbox-b1', 'active'); setBadge('pbadge-b1', 'READING', 'receiving');
      }
      break;
    case 'ab_done':
      document.getElementById('pfill-ab').style.width = '0%';
      document.getElementById('pkt-ab').className = 'pipe-pkt';
      setBadge('pbadge-a1', 'SENT', 'idle'); setBox('pbox-a1', 'success');
      pipeSent++;
      break;
    case 'bc_start':
      setBadge('pbadge-b2', 'FORWARDING', 'sending'); setBox('pbox-b2', 'active');
      const pktBC = document.getElementById('pkt-bc');
      pktBC.textContent = data.substring(0, 10);
      break;
    case 'bc_move':
      document.getElementById('pfill-bc').style.width = progress + '%';
      if (progress === 50) {
        document.getElementById('pkt-bc').className = 'pipe-pkt fly';
        setBox('pbox-c1', 'active'); setBadge('pbadge-c1', 'READING', 'receiving');
      }
      break;
    case 'bc_done':
      document.getElementById('pfill-bc').style.width = '0%';
      document.getElementById('pkt-bc').className = 'pipe-pkt';
      setBox('pbox-b2', 'success'); setBadge('pbadge-b2', 'DONE', 'idle');
      setBox('pbox-c1', 'success'); setBadge('pbadge-c1', 'RECEIVED', 'receiving');
      pipeRecv++;
      setStat('p-stat-sent', pipeSent);
      setStat('p-stat-recv', pipeRecv);
      await sleep(700);
      ['pbox-a1','pbox-b1','pbox-b2','pbox-c1'].forEach(id => setBox(id, 'active'));
      ['pbadge-a1','pbadge-b1','pbadge-b2'].forEach(id => setBadge(id, 'READY', 'sending'));
      setBadge('pbadge-c1', 'LISTEN', 'receiving');
      break;
  }
}

function resetPipeUI() {
  pipeRunning = pipeSent = pipeRecv = pipeBytes = 0;
  document.getElementById('btn-pipe-start').disabled = false;
  document.getElementById('btn-pipe-send').disabled = true;
  ['pbox-a1','pbox-b1','pbox-b2','pbox-c1'].forEach(id => setBox(id, ''));
  ['pbadge-a1','pbadge-b1','pbadge-b2','pbadge-c1'].forEach(id => setBadge(id, 'IDLE', 'idle'));
  ['pfill-ab','pfill-bc'].forEach(id => { document.getElementById(id).style.width = '0%'; });
  ['pkt-ab','pkt-bc'].forEach(id => { document.getElementById(id).className = 'pipe-pkt'; });
  ['p-stat-sent','p-stat-recv','p-stat-bytes'].forEach(id => setStat(id, '0'));
  setStat('p-stat-status', 'IDLE');
}

// ── Offline pipe simulation ───────────────────────────────────
async function simulatePipeSend() {
  const data = document.getElementById('pipe-data').value || 'DATA';
  const speed = parseInt(document.getElementById('pipe-speed').value);

  if (!pipeRunning) {
    pipeRunning = true;
    document.getElementById('btn-pipe-start').disabled = true;
    document.getElementById('btn-pipe-send').disabled = false;
    setPipesStarted();
    addLog('KERNEL', 'pipe() → fd[0]=read, fd[1]=write', 'ok');
    addLog('PROC-A', 'write-only mode', 'info');
    addLog('PROC-B', 'read+write mode (bridge)', 'info');
    addLog('PROC-C', 'read-only mode', 'info');
  }

  addLog('PROC-A', `write(fd[1], "${data}", ${data.length})`, 'send');
  setBadge('pbadge-a1', 'WRITING', 'sending'); setBox('pbox-a1', 'active');

  const pktAB = document.getElementById('pkt-ab');
  pktAB.textContent = data.substring(0, 10);
  pktAB.style.setProperty('--fly-dur', (speed / 1000) + 's');

  for (let i = 1; i <= 10; i++) {
    await sleep(speed / 10);
    document.getElementById('pfill-ab').style.width = (i * 10) + '%';
    if (i === 5) {
      pktAB.className = 'pipe-pkt fly';
      setBox('pbox-b1', 'active'); setBadge('pbadge-b1', 'READING', 'receiving');
    }
  }

  document.getElementById('pfill-ab').style.width = '0%';
  pktAB.className = 'pipe-pkt';
  setBox('pbox-a1', 'success'); setBadge('pbadge-a1', 'SENT', 'idle');
  addLog('PROC-B', `read(fd[0]) → "${data}" ✓`, 'recv');
  pipeSent++;

  await sleep(300);
  const fwd = `[FWD]${data}`;
  addLog('PROC-B', `write(fd2[1], "${fwd}")`, 'send');
  setBadge('pbadge-b2', 'FORWARDING', 'sending'); setBox('pbox-b2', 'active');
  const pktBC = document.getElementById('pkt-bc');
  pktBC.textContent = fwd.substring(0, 10);
  pktBC.style.setProperty('--fly-dur', (speed / 1000) + 's');

  for (let i = 1; i <= 10; i++) {
    await sleep(speed / 10);
    document.getElementById('pfill-bc').style.width = (i * 10) + '%';
    if (i === 5) {
      pktBC.className = 'pipe-pkt fly';
      setBox('pbox-c1', 'active'); setBadge('pbadge-c1', 'READING', 'receiving');
    }
  }

  document.getElementById('pfill-bc').style.width = '0%';
  pktBC.className = 'pipe-pkt';
  setBox('pbox-b2', 'success'); setBox('pbox-c1', 'success');
  setBadge('pbadge-b2', 'DONE', 'idle'); setBadge('pbadge-c1', 'RECEIVED', 'receiving');
  addLog('PROC-C', `read(fd2[0]) → "${fwd}" ✓`, 'recv');
  pipeRecv++;
  pipeBytes += data.length;
  setStat('p-stat-sent', pipeSent);
  setStat('p-stat-recv', pipeRecv);
  setStat('p-stat-bytes', pipeBytes);

  await sleep(700);
  ['pbox-a1','pbox-b1','pbox-b2','pbox-c1'].forEach(id => setBox(id, 'active'));
  ['pbadge-a1','pbadge-b1','pbadge-b2'].forEach(id => setBadge(id, 'READY', 'sending'));
  setBadge('pbadge-c1', 'LISTEN', 'receiving');
}

// ════════════════════════════════════════════════════════════
//  MESSAGE QUEUE
// ════════════════════════════════════════════════════════════
let mqQueue = [], mqEnq = 0, mqDeq = 0, mqDrop = 0;
const MQ_MAX = 8;

function renderQueue(d) {
  if (d) { mqQueue = d.queue; mqEnq = d.enqCount; mqDeq = d.deqCount; mqDrop = d.dropCount; }
  const track = document.getElementById('mq-track');
  // Build slots
  const needed = Math.max(MQ_MAX, mqQueue.length);
  // Clear & rebuild
  track.innerHTML = '';
  for (let i = 0; i < MQ_MAX; i++) {
    const slot = document.createElement('div');
    slot.className = 'queue-slot';
    const item = mqQueue[i];
    if (item) {
      const pc = item.priority === 'HIGH' ? 'high' : item.priority === 'LOW' ? 'low' : '';
      slot.innerHTML = `<div class="q-item ${pc}"><div class="qi-from">${item.from}</div><div class="qi-val">${item.msg}</div></div>`;
    } else {
      slot.innerHTML = `<span class="q-empty">—</span>`;
    }
    track.appendChild(slot);
  }
  setStat('mq-stat-enq', mqEnq);
  setStat('mq-stat-deq', mqDeq);
  setStat('mq-stat-depth', mqQueue.length);
  setStat('mq-stat-drop', mqDrop);
  document.getElementById('btn-mq-deq').disabled = mqQueue.length === 0;
}

// offline MQ helpers
function mqEnqueueOffline() {
  const from = document.getElementById('mq-from').value;
  const msg  = document.getElementById('mq-msg').value || 'MSG';
  const prio = document.getElementById('mq-prio').value;
  if (mqQueue.length >= MQ_MAX) {
    mqDrop++;
    addLog(from, `msgsnd() EAGAIN — queue full, "${msg}" dropped!`, 'error');
    setBox(`mq-pbox-${from==='PROC-A'?'a':from==='PROC-B'?'b':'c'}`, 'error');
    setTimeout(() => setBox(`mq-pbox-${from==='PROC-A'?'a':from==='PROC-B'?'b':'c'}`, ''), 800);
    renderQueue();
    return;
  }
  mqQueue.push({ id: Date.now(), from, msg, priority: prio });
  mqEnq++;
  addLog(from, `msgsnd("${msg}", priority=${prio}) → depth=${mqQueue.length}`, 'send');
  const key = `mq-pbox-${from==='PROC-A'?'a':from==='PROC-B'?'b':'c'}`;
  setBox(key, 'active');
  setTimeout(() => { setBox(key, 'success'); setTimeout(() => setBox(key, ''), 500); }, 300);
  renderQueue();
}

function mqDequeueOffline() {
  if (!mqQueue.length) return;
  const item = mqQueue.shift();
  mqDeq++;
  addLog('PROC-C', `msgrcv() → "${item.msg}" from ${item.from} ✓`, 'recv');
  setBox('mq-pbox-c', 'active');
  setTimeout(() => { setBox('mq-pbox-c', 'success'); setTimeout(() => setBox('mq-pbox-c', ''), 500); }, 200);
  renderQueue();
}

let mqAutoTimer = null;
function mqAutoOffline() {
  if (mqAutoTimer) { clearInterval(mqAutoTimer); mqAutoTimer = null; return; }
  const msgs = ['PING','TASK','ALERT','DATA','HB','CMD','RESULT','QUERY'];
  const procs = ['PROC-A','PROC-B'];
  const prios = ['HIGH','NORMAL','LOW'];
  let idx = 0;
  mqAutoTimer = setInterval(() => {
    if (Math.random() < 0.6 && mqQueue.length < MQ_MAX) {
      const from = procs[idx % 2], msg = msgs[idx % msgs.length], priority = prios[idx % 3];
      idx++;
      mqQueue.push({ id: Date.now(), from, msg, priority });
      mqEnq++;
      addLog(from, `AUTO msgsnd("${msg}", priority=${priority})`, 'send');
    } else if (mqQueue.length) {
      const item = mqQueue.shift(); mqDeq++;
      addLog('PROC-C', `AUTO msgrcv() → "${item.msg}" ✓`, 'recv');
    }
    renderQueue();
  }, 700);
}

// ════════════════════════════════════════════════════════════
//  SHARED MEMORY
// ════════════════════════════════════════════════════════════
let shmCells = new Array(32).fill(null);
let shmW = 0, shmR = 0, shmC = 0, mutexOn = false, mutexLocked = false;

// Build memory grid
(function buildGrid() {
  const grid = document.getElementById('mem-grid');
  for (let i = 0; i < 32; i++) {
    const cell = document.createElement('div');
    cell.className = 'mem-cell'; cell.id = `mc-${i}`;
    cell.innerHTML = `<span class="c-addr">0x${(i*4).toString(16).padStart(2,'0')}</span><span class="c-val">--</span>`;
    grid.appendChild(cell);
  }
})();

function setCellUI(idx, val, state) {
  const cell = document.getElementById(`mc-${idx}`);
  if (!cell) return;
  cell.className = `mem-cell ${state}`;
  cell.querySelector('.c-val').textContent = val === null ? '--' : val;
}

function renderSHM(d) {
  if (d.cells) {
    d.cells.forEach((c, i) => {
      if (c) setCellUI(i, c.val, c.state);
      else setCellUI(i, null, '');
    });
  }
  if (d.writes  !== undefined) { shmW = d.writes;    setStat('shm-stat-w', shmW); }
  if (d.reads   !== undefined) { shmR = d.reads;     setStat('shm-stat-r', shmR); }
  if (d.conflicts !== undefined) { shmC = d.conflicts; setStat('shm-stat-c', shmC); }
}

function updateSHMStats(d) {
  if (d.conflicts !== undefined) { shmC = d.conflicts; setStat('shm-stat-c', shmC); }
}

function shmWritingUI(proc) {
  const key = proc === 'PROC-A' ? 'a' : proc === 'PROC-B' ? 'b' : 'c';
  setBox(`shm-pbox-${key}`, 'writing');
  setBadge(`shm-pbadge-${key}`, 'WRITING', 'writing');
}

function setLock(locked, owner = '') {
  mutexLocked = locked;
  document.getElementById('lock-icon').textContent = locked ? '🔒' : '🔓';
  document.getElementById('lock-badge').className = `lock-badge ${locked ? 'locked' : 'unlocked'}`;
  document.getElementById('lock-badge').textContent = locked ? 'LOCKED' : 'UNLOCKED';
  document.getElementById('lock-owner').textContent = locked ? `Owner: ${owner}` : 'No owner';
}

function updateMutexUI(on) {
  mutexOn = on;
  const wrap = document.querySelector('.toggle-wrap');
  if (wrap) wrap.className = `toggle-wrap ${on ? 'on' : ''}`;
  const lbl = document.getElementById('mutex-label');
  lbl.textContent = on ? 'ON' : 'OFF';
  lbl.style.color = on ? 'var(--green)' : 'var(--dim)';
  const statEl = document.getElementById('shm-stat-m');
  statEl.textContent = on ? 'ON' : 'OFF';
  statEl.style.color = on ? 'var(--green)' : 'var(--dim)';
}

// offline SHM helpers
async function shmWriteOffline(proc) {
  const idx = Math.floor(Math.random() * 24);
  const val = Math.floor(Math.random() * 256);
  const valStr = `0x${val.toString(16).toUpperCase().padStart(2,'0')}`;
  const addr = `0x${(idx*4).toString(16).padStart(2,'0')}`;
  const key = proc === 'PROC-A' ? 'a' : 'b';

  if (mutexOn) {
    if (mutexLocked) {
      addLog(proc, `mutex_lock() → BLOCKED`, 'warn');
      setBox(`shm-pbox-${key}`, 'error'); setBadge(`shm-pbadge-${key}`, 'BLOCKED', 'error');
      await sleep(600); setBox(`shm-pbox-${key}`, ''); setBadge(`shm-pbadge-${key}`, 'IDLE', 'idle');
      return;
    }
    setLock(true, proc);
    addLog(proc, `mutex_lock() → acquired`, 'ok');
  }

  setBox(`shm-pbox-${key}`, 'writing'); setBadge(`shm-pbadge-${key}`, 'WRITING', 'writing');
  addLog(proc, `shm_write(addr=${addr}, val=${valStr})`, 'send');

  await sleep(400);

  if (shmCells[idx] && shmCells[idx].writer !== proc) {
    shmC++; setStat('shm-stat-c', shmC);
    addLog('KERNEL', `CONFLICT at ${addr} — ${proc} overwrites ${shmCells[idx].writer}!`, 'error');
    setCellUI(idx, valStr, 'conflict');
  } else {
    setCellUI(idx, valStr, 'written');
  }

  shmCells[idx] = { val: valStr, writer: proc, state: 'written' };
  shmW++; setStat('shm-stat-w', shmW);
  addLog(proc, `wrote ${valStr} → ${addr} ✓`, 'ok');

  setBox(`shm-pbox-${key}`, 'success'); setBadge(`shm-pbadge-${key}`, 'DONE', 'receiving');

  if (mutexOn) { setLock(false); addLog(proc, `mutex_unlock()`, 'info'); }
  await sleep(600); setBox(`shm-pbox-${key}`, ''); setBadge(`shm-pbadge-${key}`, 'IDLE', 'idle');
  setTimeout(() => { if(shmCells[idx]) setCellUI(idx, shmCells[idx].val, ''); }, 2000);
}

async function shmReadOffline() {
  addLog('PROC-C', 'shmat() reading all cells...', 'info');
  setBox('shm-pbox-c', 'active'); setBadge('shm-pbadge-c', 'READING', 'sending');
  let cnt = 0;
  for (let i = 0; i < 32; i++) {
    if (shmCells[i]) { setCellUI(i, shmCells[i].val, 'reading'); cnt++; await sleep(40); }
  }
  shmR += cnt; setStat('shm-stat-r', shmR);
  addLog('PROC-C', `Read ${cnt} cells ✓`, 'recv');
  setBox('shm-pbox-c', 'success'); setBadge('shm-pbadge-c', 'DONE', 'receiving');
  setTimeout(() => {
    shmCells.forEach((c,i) => { if(c) setCellUI(i, c.val, ''); });
    setBox('shm-pbox-c', ''); setBadge('shm-pbadge-c', 'IDLE', 'idle');
  }, 1200);
}

async function shmRaceOffline() {
  if (mutexOn) {
    addLog('KERNEL', 'Mutex ON — race condition PREVENTED ✓', 'ok');
    const rb = document.getElementById('race-banner');
    rb.className = 'race-banner show ok';
    document.getElementById('race-text').textContent = 'MUTEX ACTIVE — race condition prevented ✓';
    setTimeout(() => rb.className = 'race-banner', 3000);
    return;
  }
  const idx = Math.floor(Math.random() * 16);
  const addr = `0x${(idx*4).toString(16).padStart(2,'0')}`;
  addLog('KERNEL', `⚡ RACE: PROC-A and PROC-B writing to ${addr} simultaneously!`, 'error');
  const rb = document.getElementById('race-banner');
  rb.className = 'race-banner show';
  document.getElementById('race-text').textContent = `RACE CONDITION at ${addr} — concurrent unsynchronized writes!`;

  setBox('shm-pbox-a', 'writing'); setBadge('shm-pbadge-a', 'WRITING', 'writing');
  setBox('shm-pbox-b', 'writing'); setBadge('shm-pbadge-b', 'WRITING', 'writing');
  addLog('PROC-A', `read ${addr} → 0x00 (stale)`, 'warn');
  addLog('PROC-B', `read ${addr} → 0x00 (stale)`, 'warn');

  await sleep(350);
  setCellUI(idx, '0xAA', 'written');
  await sleep(200);
  setCellUI(idx, '0xBB', 'conflict');
  shmCells[idx] = { val: '0xBB', writer: 'PROC-B', state: 'conflict' };
  shmC++; setStat('shm-stat-c', shmC);
  addLog('RESULT', `DATA CORRUPTION: ${addr} expected 0xAA got 0xBB — lost update!`, 'error');

  await sleep(500);
  setBox('shm-pbox-a', 'error'); setBox('shm-pbox-b', 'error');
  setBadge('shm-pbadge-a', 'CONFLICT', 'error'); setBadge('shm-pbadge-b', 'CONFLICT', 'error');
  await sleep(1200);
  setBox('shm-pbox-a', ''); setBox('shm-pbox-b', '');
  setBadge('shm-pbadge-a', 'IDLE', 'idle'); setBadge('shm-pbadge-b', 'IDLE', 'idle');
}

function shmResetOffline() {
  shmCells = new Array(32).fill(null); shmW = shmR = shmC = 0;
  for (let i = 0; i < 32; i++) setCellUI(i, null, '');
  setStat('shm-stat-w', 0); setStat('shm-stat-r', 0); setStat('shm-stat-c', 0);
  ['a','b','c'].forEach(p => { setBox(`shm-pbox-${p}`, ''); setBadge(`shm-pbadge-${p}`, 'IDLE', 'idle'); });
  setLock(false);
  document.getElementById('race-banner').className = 'race-banner';
  addLog('KERNEL', 'Shared memory reset', 'info');
}

function toggleMutexOffline() {
  mutexOn = !mutexOn;
  updateMutexUI(mutexOn);
  addLog('KERNEL', `Mutex ${mutexOn ? 'ENABLED — writes serialized' : 'DISABLED — race conditions possible!'}`, mutexOn ? 'ok' : 'warn');
}

// ════════════════════════════════════════════════════════════
//  PUBLIC API — routes to backend or offline simulation
// ════════════════════════════════════════════════════════════
const API = {
  pipes: {
    start: async () => {
      if (offlineMode) { setPipesStarted(); addLog('KERNEL','pipe() created (offline sim)','ok'); return; }
      await post('/api/pipes/start');
    },
    send: async () => {
      const data  = document.getElementById('pipe-data').value  || 'DATA';
      const speed = document.getElementById('pipe-speed').value || 1400;
      if (offlineMode) { await simulatePipeSend(); return; }
      await post('/api/pipes/send', { data, speed: parseInt(speed) });
    },
    reset: async () => {
      if (offlineMode) { resetPipeUI(); addLog('KERNEL','Pipes reset (offline)','info'); return; }
      await post('/api/pipes/reset');
    }
  },

  mq: {
    enqueue: async () => {
      if (offlineMode) { mqEnqueueOffline(); return; }
      const from = document.getElementById('mq-from').value;
      const msg  = document.getElementById('mq-msg').value;
      const priority = document.getElementById('mq-prio').value;
      const res = await post('/api/mq/enqueue', { from, msg, priority });
      if (res && !res.ok && res.reason === 'QUEUE_FULL') {
        setBox('mq-pbox-a', 'error'); setTimeout(() => setBox('mq-pbox-a', ''), 800);
      }
    },
    dequeue: async () => {
      if (offlineMode) { mqDequeueOffline(); return; }
      setBox('mq-pbox-c', 'active');
      await post('/api/mq/dequeue', { receiver: 'PROC-C' });
      setTimeout(() => setBox('mq-pbox-c', ''), 600);
    },
    auto: async () => {
      if (offlineMode) { mqAutoOffline(); return; }
      await post('/api/mq/autorun');
    },
    reset: async () => {
      if (offlineMode) {
        mqQueue = []; mqEnq = mqDeq = mqDrop = 0;
        renderQueue(); addLog('KERNEL','Queue reset','info');
        if (mqAutoTimer) { clearInterval(mqAutoTimer); mqAutoTimer = null; }
        return;
      }
      await post('/api/mq/reset');
    }
  },

  shm: {
    write: async (proc) => {
      if (offlineMode) { await shmWriteOffline(proc); return; }
      const idx = Math.floor(Math.random() * 24);
      const value = Math.floor(Math.random() * 256);
      shmWritingUI(proc);
      await post('/api/shm/write', { proc, idx, value });
      await sleep(1000);
      const key = proc === 'PROC-A' ? 'a' : 'b';
      setBox(`shm-pbox-${key}`, ''); setBadge(`shm-pbadge-${key}`, 'IDLE', 'idle');
    },
    read: async () => {
      if (offlineMode) { await shmReadOffline(); return; }
      setBox('shm-pbox-c', 'active'); setBadge('shm-pbadge-c', 'READING', 'sending');
      await post('/api/shm/read');
      setTimeout(() => { setBox('shm-pbox-c', ''); setBadge('shm-pbadge-c', 'IDLE', 'idle'); }, 1200);
    },
    race: async () => {
      if (offlineMode) { await shmRaceOffline(); return; }
      await post('/api/shm/race');
    },
    toggleMutex: async () => {
      if (offlineMode) { toggleMutexOffline(); return; }
      await post('/api/shm/mutex');
    },
    reset: async () => {
      if (offlineMode) { shmResetOffline(); return; }
      await post('/api/shm/reset');
    }
  }
};

// ── Init ──────────────────────────────────────────────────────
renderQueue();
initWS();

// Start in offline mode immediately, WS will upgrade if backend is up
setTimeout(() => { if (!wsConnected) activateOfflineMode(); }, 1200);

addLog('SYSTEM', 'IPC Debugger UI loaded', 'ok');
addLog('SYSTEM', 'Attempting connection to backend...', 'info');
