# FRA Prop HQ Tradovate live worker v7.7.6

Persistent read-only WebSocket worker for near-real-time Tradovate updates. It opens one `user/syncrequest` subscription per eligible connected login, reconnects automatically, and invokes the Supabase `tradovate-live-pulse` Edge Function after provider events.

## Required environment variables

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`
- `FRA_LIVE_SYNC_WORKER_SECRET`

Optional:

- `TARGET_REFRESH_MS=30000`
- `PULSE_MIN_INTERVAL_MS=2500`
- `HEARTBEAT_MS=15000` — Supabase status heartbeat
- `TRADOVATE_HEARTBEAT_MS=2000` — Tradovate socket heartbeat; keep below 2500 ms

## v7.7.6

- Sends the required Tradovate client heartbeat frame `[]` every 2 seconds.
- Responds immediately to server heartbeat frame `h` when needed.
- Keeps the existing Supabase status heartbeat separate from the broker socket heartbeat.
- Logs the actual WebSocket close code and reason instead of only `disconnected`.
- Preserves all v7.7.5 token and `user/syncrequest` fixes.

The worker never places, modifies, or cancels orders. Do not put production secrets in the repository.
