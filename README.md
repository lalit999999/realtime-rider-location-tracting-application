# Rider Location Tracking — Final README

This repository contains a real-time rider location tracking application used for the Cohort 2026 final evaluation. It demonstrates a message-driven architecture with Kafka as the event bus, Socket.IO for real-time client updates, and MongoDB for persistence.

This README documents the implementation, setup instructions, testing steps (including an automated integration test), troubleshooting tips, privacy notes, and the grading checklist.

## Tech stack

- Node.js (ES modules)
- Express
- Socket.IO
- Kafka (kafkajs)
- MongoDB (mongoose)
- Leaflet (frontend map)
- Passport Google OAuth

## What this implements (high level)

- Clients send location updates (browser) to the server via Socket.IO.
- Server produces events to Kafka (`location-updates` topic).
- A dedicated consumer/processor consumes Kafka messages and persists them to MongoDB (`LocationEvent` model) and the Socket.IO server broadcasts updates to connected clients.
- The `LocationEvent` model includes a TTL index to expire old events automatically. TTL is configurable via `LOCATION_EVENT_TTL_SECONDS`.
- The Kafka client supports TLS/SASL and mTLS (detects certs in `api/kafka/`) for managed providers.
- Frontend uses Leaflet and the Permissions API with `watchPosition` to provide a non-intrusive mobile UX.

## File / feature map (implementation details)

- `index.js` — main Express + Socket.IO server, authentication (Passport), and HTTP APIs (includes `/api/locations`).
- `api/kafka/kafka-client.js` — builds kafkajs client with SSL/SASL/mTLS support and environment-driven config.
- `api/kafka/database-processor.js` — Kafka consumer that persists location events to MongoDB (no public NDJSON writes).
- `api/models/LocationEvent.js` — Mongoose schema (fields: userId, latitude, longitude, timestamp, partition) and TTL index.
- `public/index.html` — frontend UI (Leaflet map, geolocation flow, socket client) with mobile-friendly permission flow and deferred map init.
- `scripts/integration-test.js` — produces a test message to Kafka and polls MongoDB to verify persistence (used by CI/manual smoke test).

## Environment variables

Minimum required for local dev:

- `MONGO_URI` — MongoDB connection string (e.g., `mongodb://localhost:27017/rider`)
- `KAFKA_BROKERS` — comma-separated list, default `localhost:9092`
- `KAFKA_TOPIC` — (optional) default `location-updates`
- `APP_BASE_URL` — frontend/backend base URL (for OAuth callbacks)
- `SESSION_SECRET` — express-session secret

Optional / Kafka auth:

- `KAFKA_AUTH_METHOD` — `sasl` or `mtls` (client will prefer mTLS if cert files are present)
- Place cert files under `api/kafka/` when using mTLS: `ca.pem`, `service.cert`, `service.key`

Retention:
- `LOCATION_EVENT_TTL_SECONDS` — TTL in seconds for `LocationEvent.timestamp` (default 30 days = 2592000)

OAuth (if used):
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`

Make a `.env` file or export these in your shell for local testing.

## Local setup (quick)

1. Install dependencies:

```bash
npm install
```

2. Start services:

- Start MongoDB (local or remote)
- Start Kafka (local docker-compose included):

```bash
docker compose up -d
```

3. Set env (example):

```bash
export MONGO_URI=mongodb://localhost:27017/rider
export KAFKA_BROKERS=localhost:9092
export APP_BASE_URL=http://localhost:3300
export SESSION_SECRET=change-me
```

4. Start the app and processor (two terminals):

```bash
npm start          # starts index.js (Express + Socket)
npm run processor  # starts database-processor.js consumer
```

5. (Optional) Run the integration smoke test after the consumer is running:

```bash
npm run integration-test
```

The integration test produces one message to Kafka and polls MongoDB for the created `LocationEvent`. It returns 0 on success.

## How the flow works (quick)

1. Frontend acquires location (via `watchPosition`) and emits it to the server socket.
2. Server receives socket location update and produces it to Kafka (`location-updates`).
3. The consumer (`database-processor.js`) subscribes to the topic, parses messages, stores events into MongoDB, and the Socket.IO server broadcasts live updates to other clients.

This separation ensures the socket path stays lightweight while Kafka and the consumer handle durability and scale.

## Testing and evaluation checklist (what to verify for grading)

Automated checks:

- Integration test passes: `npm run integration-test` (Kafka + consumer + MongoDB required)
- TTL index exists: `db.locationevents.getIndexes()` in mongo shell

Manual checks:

- Login works (Google OAuth)
- Frontend map loads and `Enable Location` grants geolocation permission
- The client sends location updates and markers appear for other connected users
- Consumer logs show messages processed and MongoDB has persisted events
- Retention works when changing `LOCATION_EVENT_TTL_SECONDS` (set to a low value to test)

## Troubleshooting (common issues)

- ECONNREFUSED / Kafka connection errors

  - Cause: Kafka not running, wrong broker address, or Docker publishing issue.
  - Quick checks:

```bash
# check docker container
docker ps --filter name=kafka

# check Kafka port
nc -vz localhost 9092

# check Kafka logs
docker logs kafka --tail 200
```

  - If using a managed Kafka (Aiven), ensure `KAFKA_BROKERS` are the advertised endpoints and configure mTLS/SASL as required.

- MongoDB connection errors

  - Ensure `MONGO_URI` is correct and MongoDB is reachable.
  - Use `mongosh` or `mongo` to test connectivity.

## Privacy & repo hygiene

- The app previously wrote a public NDJSON history file. That behavior has been removed and `content/*.ndjson` is ignored in `.gitignore`.
- If `content/location-history.ndjson` was already committed and pushed, you should purge it from git history (use `git filter-repo` or `bfg`), create a backup branch first, then force-push the cleaned history. I can run that for you if you authorize and confirm collaborators are notified.

## Demo video & submission

- Record a 2–4 minute unlisted YouTube video showing: login, sending location from one client, observing updates on another, and a short explanation of Kafka → consumer → DB → Socket flow. Paste the link under the "DEMO VIDEO" section in this README.

## Final notes / lessons learned

- Keep user-sensitive data out of public files — use DB with controlled retention (TTL).
- Use Kafka for decoupling and scale: producers and consumers can scale independently.
- Prefer mTLS or SASL depending on your Kafka provider; detect and configure auth from environment and cert files.
- For mobile geolocation UX, request permissions once and use `watchPosition` to avoid repeated intrusive prompts.

If you'd like, I can now:

- Purge `content/location-history.ndjson` from git history (destructive rewrite) — I will create a backup branch first.
- Run the integration test here (requires Kafka + MongoDB running in this environment).
- Help you produce a short demo script for recording the video.

---

DEMO VIDEO: [Add your unlisted link here]

Author: Lalit Gujar — Cohort 2026

