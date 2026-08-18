# Meeting Webhook Service

An event-driven webhook ingestion service for a video meeting platform, built with Java 17 and
Spring Boot 4. It receives meeting lifecycle and live-transcription webhooks, processes them
asynchronously through Kafka, persists the results to PostgreSQL, reconstructs the full transcript
when a meeting ends, and exposes a read API to retrieve the ordered transcript for a session.

> Design rationale, trade-offs, and per-decision detail live in [`DESIGN.md`](DESIGN.md).

---

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [API reference](#api-reference)
- [Demo UI & Simulation](#demo-ui--simulation)
- [Scalability & service boundaries](#scalability--service-boundaries)
- [Trade-offs & assumptions](#trade-offs--assumptions)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing](#testing)
- [Edge-case behavior](#edge-case-behavior)
- [Tech stack](#tech-stack)
- [Future work](#future-work)

---

## Architecture

![High-level architecture](docs/architecture-simple.png)

**Key ideas**

- **Thin, fast ingestion.** The webhook verifies the signature over the raw bytes, does minimal
  envelope validation, and publishes the raw JSON to Kafka keyed by `sessionId`, then returns
  `202 Accepted`. All real work happens asynchronously in consumers.
- **Ordering by partition key.** Keying every event by `sessionId` guarantees per-session ordering
  on a single partition — important for the started → transcript → ended lifecycle.
- **Idempotent handlers.** Kafka is at-least-once, so every handler tolerates duplicates
  (session `existsById`, transcript `transcriptId` dedup + unique constraint, ENDED no-op).
- **Reconstruction as its own event.** Ending a session publishes a `transcript.reconstruct` task,
  keeping the DB write fast and letting assembly retry independently.
- **Pluggable storage.** `StorageService` abstracts where transcripts are written; the local
  implementation uses files, and an object store could drop in without touching reconstruction.

**Layering (package-by-feature).** The top-level packages are business slices, not technical
layers — each slice owns its controllers, services, entities, and repositories:

```
ai.soulside
├── webhook/            Ingestion: HTTP edge, DTOs, HMAC, Kafka producer
│   ├── dto/            WebhookEnvelope, WebhookEventType
│   └── security/       HmacSignatureVerifier
├── meeting/            Meeting/Session domain
│   ├── event/          MeetingStartedEvent, MeetingEndedEvent
│   ├── model/          Meeting, Session, SessionStatus
│   └── repository/     MeetingRepository, SessionRepository
├── transcript/         Transcript domain: ingest, reconstruction, read API
│   ├── dto/            TranscriptResponse, TranscriptEntry
│   ├── event/          MeetingTranscriptEvent
│   ├── model/          TranscriptSegment
│   └── repository/     TranscriptSegmentRepository
├── storage/            Persistence port + adapter (StorageService, LocalFileStorageService)
└── common/             Cross-cutting (shared kernel)
    ├── config/         Kafka, AppProperties
    └── web/            GlobalExceptionHandler, ApiError
```

- **`webhook`** — the ingestion edge: verifies HMAC, validates the envelope, and publishes to Kafka.
  It owns no database.
- **`meeting`** — the Meeting/Session lifecycle: handles `meeting.started`/`meeting.ended`, owns the
  meeting and session tables.
- **`transcript`** — everything about transcript segments: ingest + dedup, reconstruction into a
  file, and the read/query API.
- **`storage`** — a driven port (`StorageService`) with a filesystem adapter today, swappable for S3.
- **`common`** — cross-cutting concerns shared across slices (correlation IDs, config, error handling,
  metrics) — the equivalent of a shared kernel, with no business logic.

Within each slice we keep hexagonal layering: controllers/consumers are thin adapters, services hold
the business logic, and repositories/storage are driven ports. So it's package-by-feature at the top,
ports-and-adapters inside each slice.

---

## Quick start

Requires **Docker** + **Docker Compose**. Everything else (JDK, Maven, Kafka, Postgres) is either
provided by the wrapper or the containers.

```bash
# Build the app image and start the full stack (Postgres, Kafka, Kafka UI, Prometheus, Grafana, app)
docker compose up --build
```

Wait for the app to report healthy, then:

```bash
# Drive a full meeting lifecycle and print the verify command
./scripts/simulate_meeting.sh

# Retrieve the reconstructed transcript
curl -s http://localhost:8080/api/v1/meetings/50c8940e-1b97-402a-97d6-2708b7feca41/sessions/05e57591-d89e-45c9-ae44-08dc1eaad0e0/transcript | jq .
```

Recommended: Open Demo UI at <http://localhost:8080/webhook-tester.html>.

### Service endpoints

| Service     | URL                                   | Notes                          |
| ----------- | ------------------------------------- | ------------------------------ |
| Application | <http://localhost:8080>               | REST API + testing UI          |
| Health      | <http://localhost:8080/actuator/health> |                              |
| Prometheus metrics | <http://localhost:8080/actuator/prometheus> |                     |
| Kafka UI    | <http://localhost:8090>               | topics, messages, DLQ          |
| Prometheus  | <http://localhost:9090>               | targets, queries               |
| Grafana     | <http://localhost:3000>               | "Meeting Webhook Service" dashboard (anonymous view enabled) |

The `local` profile points at `localhost:5432` (Postgres) and `localhost:29092` (Kafka's external
listener) and disables HMAC verification for convenience.

---

## API reference

### `POST /api/v1/webhooks`

Accepts `meeting.started`, `meeting.transcript`, and `meeting.ended` events (routed by the `event`
field). Returns **202 Accepted** on success.

- **400** — malformed JSON or missing required envelope fields (`event`, `meeting.id`,
  `meeting.sessionId`).
- **401** — missing/invalid `X-Signature` when HMAC verification is enabled (fail-closed).

Example (`meeting.started`):

```json
{
  "event": "meeting.started",
  "meeting": {
    "id": "50c8940e-1b97-402a-97d6-2708b7feca41",
    "sessionId": "05e57591-d89e-45c9-ae44-08dc1eaad0e0",
    "title": "Q4 Planning Sync",
    "startedAt": "2024-12-13T06:57:09.736Z",
    "createdAt": "2024-12-13T06:57:09.736Z",
    "organizedBy": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" }
  }
}
```

Transcript `data.startOffset` / `data.endOffset` are **relative seconds** from session start. Both
plain seconds (`"300"`) and clock offsets (`"00:05:00"`) are accepted and normalized to whole seconds.

### `GET /api/v1/meetings/{meetingId}/sessions/{sessionId}/transcript`

Returns the complete, ordered transcript plus session metadata. **404** if the meeting/session pair
is unknown. Works for both LIVE (still accumulating) and ENDED (reconstructed) sessions.

```json
{
  "meetingId": "…", "meetingTitle": "Q4 Planning Sync",
  "sessionId": "…", "status": "ENDED",
  "startedAt": "…", "endedAt": "…",
  "transcriptUri": "file:///…/{sessionId}.txt",
  "segmentCount": 3,
  "entries": [
    { "sequenceNumber": 1, "speakerId": "…", "speakerName": "Alice",
      "content": "…", "startOffsetSeconds": 2, "endOffsetSeconds": 5, "language": "en" }
  ]
}
```

---

## Demo UI & Simulation

A single-page tester is served at **`/webhook-tester.html`**. It provides:

- **Full session simulation** — one click runs `meeting.started` → N transcripts → `meeting.ended`,
  then polls the GET endpoint and shows the reconstructed transcript.
- **Edge case scenarios** — six buttons mirroring `scripts/simulate_edge_cases.sh` (duplicate chunk,
  out-of-order, transcript-after-ended, ended-without-started, concurrent sessions, malformed payload).
- **Manual single-event tester** — send any hand-edited payload to the webhook.


### Simulation scripts (Optional)

| Script                              | Purpose                                                        |
| ----------------------------------- | -------------------------------------------------------------- |
| `scripts/simulate_meeting.sh`       | Happy-path lifecycle (started → 3 transcripts → ended).        |
| `scripts/simulate_edge_cases.sh`    | Six edge-case scenarios with explanatory output.               |

```bash
./scripts/simulate_meeting.sh          # defaults to http://localhost:8080
./scripts/simulate_edge_cases.sh       # optional first arg overrides the base URL
```

---

## Scalability & service boundaries

The service is a modular monolith with boundaries drawn so it can be split into independently
deployable services without a rewrite — components already communicate over Kafka topics and
repository/port interfaces, not cross-domain method calls.

- **Read and write paths are separated.** The write path (webhook → Kafka → consumers) is fully
  asynchronous and scales with consumer concurrency / partitions; the read path
  (`GET .../transcript`) only touches the DB and scales with read replicas / caching. They fail and
  scale independently — a lean CQRS split over one store.
- **Natural extraction points:** ingestion (HTTP edge, stateless) · processing (event consumers) ·
  reconstruction (assembly + storage) · read/query. Each is a package today with its own
  topic/table ownership.
- **Broker swap is config, not code:** publishing goes through `KafkaProducerService` and consumption
  through `@KafkaListener`, so moving to managed Kafka is a configuration change.

See [`DESIGN.md`](DESIGN.md) for the full boundary analysis and the per-decision rationale.

## Trade-offs & assumptions

- **In-process Kafka topics, not a managed broker.** Sufficient for the assignment; the producer and
  `@KafkaListener` seams mean swapping to managed Kafka is configuration, not a rewrite.
- **No transactional outbox — natural-key idempotency instead.** Ingestion doesn't write to the DB
  (no dual-write to make atomic), and consumers dedup on `transcriptId` / `sessionId`, so at-least-once
  redelivery is a safe no-op. A lost `transcript.reconstruct` task (or a late transcript after
  `meeting.ended`) can leave the file missing/stale; the planned fix is to **self-heal on read** —
  if a session is `ENDED` but `transcriptUri` is null or stale, requeue reconstruction (idempotent) and
  serve the DB-sourced transcript meanwhile. Readers are always correct since the DB is the source of
  truth.
- **Reconstruct after `meeting.ended`, relying on at-least-once client delivery.** Assembling once the
  session closes avoids rewriting the file per chunk and needing a total count up front. Completeness
  can be checked via `sequenceNumber` gap detection at reconstruction; a `totalSegments` field on the
  closing event would make it definitive if upstream added one.
- **Local file storage simulates an object store.** `LocalFileStorageService` sits behind the
  `StorageService` port; an `S3StorageService` (with SSE encryption at rest and gzip compression) is a
  drop-in swap with no change to reconstruction or the read API.
- **Raw payload forwarded to Kafka.** The webhook doesn't fully type the body before publishing —
  keeps ingestion fast and decouples HTTP from event schemas; the consumer owns deserialization.
- **DB is the source of truth for reads.** The GET endpoint always orders segments from the DB; the
  stored file is a convenience artifact referenced by URI, not reverse-parsed. It returns metadata +
  URL + full `entries[]` inline today; at scale this would become tiered — a presigned S3 URL for
  completed sessions, with DB-sourced `entries[]` as the fallback/streaming path.
- **`sessionId` is the unit of ordering and idempotency.** Assumed globally unique.

---

## Configuration

All app-specific settings sit under the `app.*` prefix (bound in `AppProperties`).

| Property / env var | Default | Meaning |
| ------------------ | ------- | ------- |
| `app.webhook.signature-verification-enabled` / `WEBHOOK_SIGNATURE_ENABLED` | `true` (`false` in `local`) | Enforce HMAC verification (fail-closed). |
| `app.webhook.hmac-secret` / `WEBHOOK_HMAC_SECRET` | `change-me-in-production` | Shared secret for HMAC-SHA256. |
| `app.storage.base-path` | `./data/transcripts` | Directory for assembled transcript files. |
| `app.kafka.retry.*` | `1s / ×5 / 30s cap / 3 attempts` | Consumer retry backoff (tests use fast values). |
| `LOG_FORMAT` | _(empty)_ | Set to `ecs` for structured JSON logs; empty = readable console. |

Profiles: **`local`** (Docker infra) · **`test`** (H2 + embedded Kafka).

---

## Observability

- **Correlation IDs** — `X-Correlation-Id` is minted/propagated across HTTP → Kafka headers →
  consumer MDC, so a single request is traceable end to end.
- **Structured logging** — opt-in JSON via `LOG_FORMAT`; `correlationId`, `sessionId`, and `event`
  are carried in the MDC.
- **Custom metrics** — `webhook.events.received`, `consumer.events.processed` (outcome-tagged),
  `consumer.event.processing.time`, `transcript.reconstruction.count`, `kafka.consumer.dlq.count`,
  exposed at `/actuator/prometheus` and visualized by the provisioned Grafana dashboard.

---

## Testing

```bash
./mvnw test           # unit + integration (embedded Kafka + H2), 61 tests
./mvnw clean verify   # full build + tests + bootable jar
```

Two levels, run with no external infrastructure — **unit tests** exercise service logic in isolation
with Mockito; **integration tests** exercise the real HTTP → Kafka → DB flow against an embedded Kafka
broker and H2, with async assertions via Awaitility.

**Unit tests** — deterministic logic and its failure modes:

- **Idempotency & dedup** — duplicate `meeting.started` creates one session; duplicate `transcriptId`
  is skipped; a replayed `meeting.ended` on an already-ENDED session is a no-op.
- **Fail-fast validation** — malformed/incomplete payloads (null `data`, missing `sessionId`, etc.)
  raise a clear, non-retryable error rather than a deep NPE.
- **Ordering & assembly** — reconstruction renders segments in `sequenceNumber` order; the read query
  returns them ordered; unknown meeting/session → 404.
- **Parsing & security** — offset parsing (plain seconds vs `HH:MM:SS`); HMAC signatures accepted,
  rejected on tamper, rejected when missing (fail-closed).

**Integration tests** — end-to-end behavior and infrastructure failure modes:

- **Happy path** — full lifecycle → transcript reconstructed to storage and served by the GET API.
- **Delivery hazards** — out-of-order transcripts still read in order; duplicate deliveries stored
  once; a transcript arriving after `meeting.ended` is still persisted.
- **Poison messages & retries** — `meeting.ended` for an unknown session is retried then routed to the
  DLQ; a malformed payload dead-letters *fast* (non-retryable) instead of exhausting the retry budget.
- **Concurrency** — concurrent sessions of the same meeting are tracked independently.
- **Contracts** — webhook returns 202/400/401 as expected; persistence constraints and ordering hold;
  custom metrics are exposed on `/actuator/prometheus`.

---

## Edge-case behavior

Summarized here; full rationale and the mapping to tests is in [`DESIGN.md`](DESIGN.md).

| Scenario | Behavior |
| -------- | -------- |
| Duplicate transcript chunk | Stored once (dedup on `transcriptId` + unique constraint). |
| Out-of-order transcripts | Stored as-is; ordered by `sequenceNumber` on read. |
| Duplicate `meeting.started` | Meeting refreshed; session created once. |
| Transcript after `meeting.ended` | Still stored (no data loss); session stays ENDED; visible via GET. |
| `meeting.ended` without `meeting.started` | Retried, then dead-lettered. |
| Concurrent sessions, same meeting | Tracked independently under one meeting. |
| Malformed payload (null fields) | 202 at edge; consumer fails fast (non-retryable) → immediate DLQ. |
| Malformed JSON at webhook | 400 with a meaningful message. |
| Missing/invalid HMAC (enabled) | 401, fail-closed. |

### Edge-case sequences

One sequence diagram per scenario (see [`DESIGN.md`](DESIGN.md) for the rationale):

- [Happy-path sequence diagram](docs/sequence-happy-path.png)
- [Duplicate transcript chunk](docs/sequence-duplicate-transcript.png) — same `transcriptId` twice → stored once
- [Out-of-order delivery](docs/sequence-out-of-order.png) — seq 3, 1, 2 → ordered on read
- [Transcript after `meeting.ended`](docs/sequence-transcript-after-ended.png) — still stored, session stays ENDED
- [`meeting.ended` without `meeting.started`](docs/sequence-ended-without-started.png) — retried, then dead-lettered
- [Concurrent sessions, same meeting](docs/sequence-concurrent-sessions.png) — tracked independently

---

## Tech stack

| Concern      | Choice                                               |
| ------------ | ---------------------------------------------------- |
| Language     | Java 17                                              |
| Framework    | Spring Boot 4.1.0 (Spring Framework 7)               |
| Build        | Maven (via the included wrapper `./mvnw`)            |
| Messaging    | Apache Kafka (KRaft mode — no Zookeeper)             |
| Database     | PostgreSQL 16 (H2 in tests)                          |
| Migrations   | Flyway                                               |
| Boilerplate  | Lombok 1.18.46                                        |
| Observability| Actuator + Micrometer/Prometheus + Grafana           |
| Tests        | JUnit 5, Mockito, Spring Kafka Test (embedded), Awaitility |

---

## Future work

- Split into microservices along the documented boundaries (ingestion / processing / reconstruction /
  read) when independent scaling or team ownership justifies it.
- `S3StorageService` with SSE encryption at rest and gzip compression, behind the existing
  `StorageService` port.
- Sequence-gap completeness detection at reconstruction; adopt a `totalSegments` field on
  `meeting.ended` if the upstream contract adds one.
- Self-healing reconstruction: requeue on read (and via a sweeper) when a session is `ENDED` but the
  transcript file is missing or stale — closes the lost-task / incomplete-file gaps.
- Tiered read response: presigned S3 URL for completed sessions, `entries[]` as the fallback.
- External secret management and a schema registry for typed events.
- Read replicas / caching for the transcript read endpoint; pagination for very large sessions.
- An outbox or Kafka transactions on the reconstruct hop if guaranteed delivery of that task is needed.
