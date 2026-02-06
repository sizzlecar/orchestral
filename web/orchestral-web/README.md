# orchestral-web

Web client for Orchestral server, implemented with React + Vite + TypeScript + TanStack Query + SSE.

## Prerequisites

1. Start server:

```bash
cargo run -p orchestral-server -- --config configs/orchestral.cli.yaml --listen 127.0.0.1:8080
```

2. In this directory, install deps:

```bash
npm install
```

## Run

```bash
npm run dev
```

Open http://127.0.0.1:5173.

## Config

- `VITE_API_BASE` (optional)
  - Default: empty, requests go to same-origin (dev proxy forwards to `127.0.0.1:8080`)
