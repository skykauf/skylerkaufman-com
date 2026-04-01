# skylerkaufman.com

Personal site with a canvas background, **Volley Chat** (local LLM via [Ollama](https://ollama.com)), and optional static hosting.

## Local LLM (no external APIs)

Chat talks only to **Ollama** on the same machine (default `http://127.0.0.1:11434`). The app never calls OpenAI or other hosted LLM APIs.

1. Install and start Ollama, then pull a model (example):

   ```bash
   ollama pull llama3.2
   ```

2. Install dependencies and run the site + API:

   ```bash
   npm install
   npm start
   ```

3. Open [http://localhost:3000](http://localhost:3000). Click the **ringed planet** (or go to `/volley-chat/`).

Optional env (see `.env.example`):

- `PORT` — server port (default `3000`)
- `OLLAMA_URL` — Ollama base URL
- `OLLAMA_MODEL` — model name (default `llama3.2`)

## Deploying

**Vercel (static only):** The default “static site” deploy serves HTML/CSS/JS but **not** `server.js` or `/api/chat`. Volley Chat needs a host where you run Node and Ollama together (your own VPS, homelab, etc.), or you drop the chat feature on pure static hosting.

**Self‑hosted:** Run `npm start` behind a reverse proxy (e.g. Caddy or nginx), keep Ollama on localhost or a private URL only the Node app can reach.

## Pure static on Vercel (original flow)

1. Import the repo in Vercel, framework **Other**, output the repo root.
2. Add your domain under **Settings → Domains** and set DNS at your registrar.

```bash
npx vercel
```
