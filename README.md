# FRA Prop HQ Tradovate live worker v7.7.2

Persistent WebSocket worker for near-real-time Tradovate updates. It opens one read-only `user/syncrequest` session per eligible connected login, reconnects automatically, and invokes the Supabase `tradovate-live-pulse` Edge Function after account/fill events. It never places, modifies, or cancels orders.

## Required environment variables

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY` — recommended current Supabase server-side secret key
  - Legacy fallback supported: `SUPABASE_SERVICE_ROLE_KEY`
- `FRA_LIVE_SYNC_WORKER_SECRET` — a new long random value shared with the Edge Function

Optional:

- `TARGET_REFRESH_MS=30000`
- `PULSE_MIN_INTERVAL_MS=2500`
- `HEARTBEAT_MS=15000`

Deploy the included Dockerfile as a persistent background worker on Render, Railway, Fly.io or another always-on container host. Do not put production secrets in the repository or website files.

## Security design

The Render worker never receives `TOKEN_ENCRYPTION_KEY` and never reads encrypted provider credentials directly. The Supabase `tradovate-live-pulse` function keeps the existing encryption key inside Supabase, decrypts or renews the stored Tradovate token, and returns a short-lived live session only to a worker presenting the shared `FRA_LIVE_SYNC_WORKER_SECRET`.
