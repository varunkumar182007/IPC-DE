# IPC Debugger — Full Stack

A full-stack Inter-Process Communication visualizer with:
- **Backend**: Node.js + Express REST API + WebSocket server
- **Frontend**: Vanilla HTML/CSS/JS with real-time WebSocket updates

---

## Project Structure

```
ipc-fullstack/
├── backend/
│   ├── server.js       ← Express + WebSocket IPC engine
│   └── package.json
└── frontend/
    └── public/
        ├── index.html  ← Main UI
        ├── style.css   ← Stylesheet
        └── script.js   ← API client + offline simulation
```

---

## Setup & Run

### 1. Install backend dependencies
```bash
cd backend
npm install
```

### 2. Start the backend
```bash
node server.js
# OR with auto-reload:
npm run dev
```

Backend runs on **http://localhost:4000**

### 3. Open the frontend
Open `frontend/public/index.html` in your browser.

Or serve it via the backend (it auto-serves the `frontend/public` folder):
```
http://localhost:4000
```

---

## Features

### Pipes Tab
- Start a pipe simulation (creates two chained pipes A→B→C)
- Send custom data with animated real-time packet flow
- Adjustable speed, buffer fill indicator, byte stats

### Message Queue Tab
- Manual enqueue/dequeue with priority (HIGH / NORMAL / LOW)
- Choose producer process and message content
- Auto-run mode: automated producers and consumer
- Visual FIFO queue with live depth tracking
- Dropped message detection (queue full)

### Shared Memory Tab
- PROC-A and PROC-B write to random memory cells
- Read All: PROC-C reads the entire segment
- Race Condition demo: concurrent writes without mutex → data corruption
- Mutex toggle: enable to serialize writes and prevent races
- 32-cell memory grid with color-coded states

### Kernel Log (sidebar)
- Real-time log of all syscalls and events
- Pause / clear controls
- Color-coded by severity (info / ok / send / recv / warn / error)

---

## How It Works

### Backend (server.js)
- **REST API**: Each IPC action (start pipe, enqueue, write) is a POST endpoint
- **WebSocket**: Server broadcasts real-time events to all connected clients
- **Async simulation**: `async/await` + `setTimeout` simulate timing and delays

### Frontend (script.js)
- Connects via WebSocket for live updates
- Falls back to **offline simulation mode** automatically if backend is unavailable
- All UI state is driven by WebSocket events from the backend

### Key API Endpoints
| Method | Path | Description |
|--------|------|-------------|
| POST | /api/pipes/start | Initialize pipe file descriptors |
| POST | /api/pipes/send | Send data through chained pipes |
| POST | /api/pipes/reset | Close and reset pipes |
| POST | /api/mq/enqueue | msgsnd() — add message to queue |
| POST | /api/mq/dequeue | msgrcv() — remove from queue |
| POST | /api/mq/autorun | Run automated producer/consumer |
| POST | /api/shm/write | Write to shared memory cell |
| POST | /api/shm/read | Read all cells |
| POST | /api/shm/race | Trigger race condition demo |
| POST | /api/shm/mutex | Toggle mutex on/off |
| POST | /api/shm/reset | Clear shared segment |

---

## Tech Stack
- **Backend**: Node.js, Express 4, ws (WebSocket), cors
- **Frontend**: Vanilla JS (no framework), IBM Plex Mono + Syne fonts
- **Protocol**: REST for actions, WebSocket for real-time events
