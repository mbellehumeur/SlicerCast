# Cast Hub + VolView Run Guide

This project includes a FastAPI-based Cast Hub and an optional VolView Python RPC layer.

## Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/)
- Node.js + npm

## Start from the VolView repo root

Use two terminals.

## Terminal 1: Start hub / server

```bash
cd server
poetry install
```

### Option A: Cast Hub + VolView RPC together (recommended)

```bash
poetry run python server_with_hub.py --port 4014 ./examples/example_api.py
```

### Option B: Cast Hub only

```bash
poetry run python server_with_hub.py --port 4014
```

### Option C: VolView RPC only (no Cast Hub)

```bash
poetry run python -m volview_server -P 4014 ./examples/example_api.py
```

## Terminal 2: Start VolView frontend

```bash
cd ..
npm install
npm run dev
```

Open the URL printed by Vite (usually `http://localhost:5173`).

## Useful URLs (default port 4014)

- Hub Admin: `http://localhost:4014/api/hub/admin`
- Hub API: `http://localhost:4014/api/hub/`
- Hub Status: `http://localhost:4014/api/hub/status`
- OAuth Token endpoint: `http://localhost:4014/oauth/token`

If using combined mode (`server_with_hub.py` with an API script), Socket.IO RPC is available at:

- `ws://localhost:4014/socket.io/`

## VolView environment hint

Set remote server URL in `.env.local`:

```env
VITE_REMOTE_SERVER_URL=http://localhost:4014
```

If `npm run dev` is already running, restart it after changing `.env.local`.

## Deploy to Azure Web App (ZIP)

This section deploys the combined Cast Hub + VolView RPC server to Azure App Service.

### 1) Prepare deployment package

From `VolView/server`:

```bash
cd server
poetry install
poetry export -f requirements.txt --output requirements.txt --without-hashes
```

Create a zip file from the **contents** of `server` (not the parent folder).  
Your zip root should include:

- `server_with_hub.py`
- `cast_api/`
- `volview_server/`
- `examples/`
- `requirements.txt`

### 2) Create Azure App Service (Linux / Python)

```bash
az group create -n rg-volview-cast -l westeurope
az appservice plan create -g rg-volview-cast -n plan-volview-cast --is-linux --sku B1
az webapp create -g rg-volview-cast -p plan-volview-cast -n <unique-app-name> --runtime "PYTHON:3.11"
```

### 3) Configure startup command

Use port `8000` and bind to `0.0.0.0`:

```bash
az webapp config set -g rg-volview-cast -n <unique-app-name> --startup-file "python server_with_hub.py --host 0.0.0.0 --port 8000 ./examples/example_api.py"
```

Enable build during deploy:

```bash
az webapp config appsettings set -g rg-volview-cast -n <unique-app-name> --settings SCM_DO_BUILD_DURING_DEPLOYMENT=true
```

### 4) Deploy the ZIP

```bash
az webapp deploy -g rg-volview-cast -n <unique-app-name> --src-path ./server.zip --type zip
```

### 5) Verify deployment

- Hub status: `https://<unique-app-name>.azurewebsites.net/api/hub/status`
- Hub admin: `https://<unique-app-name>.azurewebsites.net/api/hub/admin`

Use these endpoints in clients:

- Hub endpoint: `https://<unique-app-name>.azurewebsites.net/api/hub`
- Token endpoint: `https://<unique-app-name>.azurewebsites.net/oauth/token`
- Remote server URL (`VITE_REMOTE_SERVER_URL`): `https://<unique-app-name>.azurewebsites.net`
