# Design Notes — Event-Driven Meeting Webhook Service

This document captures the assumptions, trade-offs, decisions, and architecture choices made
while building the service. It is updated at the end of each phase.

---

## Architecture Overview

```
                      ┌─────────────────┐
   Webhook POST  ───► │ WebhookController│  (verify HMAC, validate envelope, 202)
                      └────────┬────────┘
                               │ publish raw JSON, key = sessionId
                               ▼
                      ┌─────────────────┐
                      │  meeting.events │  (Kafka topic, 3 partitions)
                      └────────┬────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ MeetingEventConsumer  │ route by eventType header
                    └───┬───────────┬───────┘
        meeting.started │           │ meeting.transcript
        meeting.ended   │           │
                        ▼           ▼
                 ┌────────────┐  ┌──────────────────┐
                 │MeetingSvc  │  │ TranscriptService │
                 └─────┬──────┘  └────────┬─────────┘
                       │                  │
                       ▼                  ▼
                 ┌──────────────────────────────┐
                 │        PostgreSQL             │
                 │ meetings / sessions / segments│
                 └──────────────────────────────┘
                       │ (on meeting.ended)
                       ▼
              ┌─────────────────────┐
              │transcript.reconstruct│  (Kafka topic)
              └──────────┬──────────┘
                         ▼
              ┌───────────────────────────┐
              │TranscriptReconstructConsumer│ assemble ordered segments
              └──────────┬────────────────┘
                         ▼
                 ┌───────────────┐
                 │ StorageService │ → {basePath}/{sessionId}.txt, URI on session
                 └───────────────┘
```

Failures in either consumer are retried with backoff by a `DefaultErrorHandler`, then routed to
a `<topic>.DLT` dead-letter topic.

**Read path:** `GET /api/v1/meetings/{id}/sessions/{sessionId}/transcript` →
`MeetingReadController` → `TranscriptQueryService` → ordered segments from PostgreSQL → JSON.

### Layering / Package Structure

| Package      | Responsibility                                             |
| ------------ | ---------------------------------------------------------- |
| `webhook`    | HTTP ingestion, DTOs, HMAC verification, Kafka producer    |
| `meeting`    | Meeting/Session domain: entities, repos, service, consumer |
| `transcript` | Transcript segment domain: entity, repo, service           |
| `storage`    | Storage abstraction (Phase 6)                              |
| `common`     | Cross-cutting: correlation IDs, config, error handling     |

Business logic lives in services; controllers and consumers are thin adapters. Infrastructure
concerns (Kafka, HMAC, correlation IDs) are isolated from domain logic.

---

## Tech Baseline

| Choice                | Decision                        | Rationale                                                       |
| --------------------- | ------------------------------- | -------------------------------------------------------------- |
| Framework             | Spring Boot 4.1.0               | Latest; requested. Java 17 baseline, Spring Framework 7.        |
| Build                 | Maven (+ wrapper)               | Requested. Wrapper makes the build self-contained (no local mvn). |
| Language boilerplate  | Lombok 1.18.46                  | Requested. Overridden from the BOM version for JDK 24 support. |
| Database              | PostgreSQL 16 (H2 in tests)     | Relational fit for the meeting→session→segment hierarchy.      |
| Messaging             | Kafka (KRaft, no Zookeeper)     | Event-driven core; partition-keyed ordering per session.       |
| Migrations            | Flyway                          | Versioned, reviewable schema.                                  |
| Observability         | Actuator + Micrometer/Prometheus + Grafana | Metrics and health out of the box.                  |

### Spring Boot 4 / Jackson 3 migration notes (gotchas encountered)

- `spring-boot-starter-web` → **`spring-boot-starter-webmvc`**.
- Kafka starter is now first-party: **`spring-boot-starter-kafka`**.
- Boot 4 ships **Jackson 3**: `ObjectMapper` is `tools.jackson.databind.ObjectMapper`
  (annotations remain under `com.fasterxml.jackson.annotation`).
- `@DataJpaTest` moved to `org.springframework.boot.data.jpa.test.autoconfigure`
  (**`spring-boot-starter-data-jpa-test`**).
- `@AutoConfigureMockMvc` / `@WebMvcTest` moved to `org.springframework.boot.webmvc.test.autoconfigure`
  (**`spring-boot-starter-webmvc-test`**).
- Spring Framework 7 removed `ExponentialBackOffWithMaxRetries`; `ExponentialBackOff` now has
  `setMaxAttempts(long)` built in.

---

## Domain Model

- **Meeting** (1) → (*) **Session** (1) → (*) **TranscriptSegment**.
- `Session.status` ∈ {`LIVE`, `ENDED`} (enum stored as string, plus a DB CHECK constraint).
- `TranscriptSegment` carries `transcriptId` (natural dedup key, `UNIQUE`), `sequenceNumber`
  (ordering), speaker id/name, content, offsets (integer seconds), language.
- Index on `transcript_segments(session_id, sequence_number)` supports ordered reconstruction.
- `Session.transcriptUri` records where the assembled transcript was stored (set on reconstruction).

### Offsets

`startOffset`/`endOffset` are stored as **integer seconds** relative to session start. The upstream
payload is inconsistent — the assignment text describes plain seconds (`"300"`) while the sample
script sends clock offsets (`"00:00:02.100"`). `OffsetParser` accepts both forms and normalizes to
truncated whole seconds, so the DB stays a clean numeric type regardless of input format.

### Lombok on entities

Entities use `@Getter/@Setter/@Builder/@NoArgsConstructor/@AllArgsConstructor` but **not**
`@Data`/`@EqualsAndHashCode`. `equals`/`hashCode` over lazy JPA associations is a well-known
footgun (can trigger lazy loading or infinite recursion), so it is deliberately avoided.

---

## Event-Driven Design

- **Why Kafka:** the assignment is explicitly event-driven. Kafka gives durable, replayable,
  partition-ordered delivery. Keying every event by `sessionId` guarantees all events for a
  session land on one partition and are processed in order — critical for transcript sequencing
  and for the started→transcript→ended lifecycle.
- **Producer keeps it thin:** the webhook publishes the *raw* JSON body verbatim (keyed by
  sessionId) with `eventType` and `correlationId` headers. Per-event deserialization happens in
  the consumer. This keeps ingestion fast and decouples the HTTP layer from event schemas.
- **At-least-once + idempotency:** Kafka is at-least-once, so every handler is idempotent
  (see below). Duplicates are expected and safe.
- **Swapping the broker:** because publishing goes through a small `KafkaProducerService` and
  consumption through `@KafkaListener`, moving to a managed Kafka / another broker is a config
  change, not a code rewrite.

---

## Idempotency & Ordering

| Event              | Idempotency strategy                                                        |
| ------------------ | --------------------------------------------------------------------------- |
| `meeting.started`  | Upsert Meeting; create Session only if `existsById(sessionId)` is false.    |
| `meeting.transcript` | Skip if `existsByTranscriptId`; DB `UNIQUE(transcript_id)` is the race backstop. |
| `meeting.ended`    | No-op if session already `ENDED`.                                           |

Ordering is by `sequenceNumber` at read time, so out-of-order delivery still reconstructs
correctly. Partition keying by sessionId makes out-of-order delivery rare in practice.

---

## Error Handling & Retries

- `DefaultErrorHandler` + `ExponentialBackOff` (prod defaults 1s → 5s → 30s, 3 attempts;
  configurable via `app.kafka.retry.*`; tests use 200ms ×2 for speed).
- `DeadLetterPublishingRecoverer` routes exhausted records to `<topic>.DLT`.
- **Retryable vs not:** transient/ordering failures (e.g. `UnknownSessionException` — a
  transcript/ended event that arrived before its session committed) are retried. Structural
  failures (`MismatchedInputException`, `IllegalArgumentException` — malformed payloads) are
  marked non-retryable and dead-lettered immediately, since retrying will never help.
- **Fail-fast validation in handlers:** `MeetingService`/`TranscriptService` validate required
  fields (`meeting.sessionId`, `data.transcriptId`, `sequenceNumber`, `content`, offsets, etc.)
  at the top of each handler and throw `IllegalArgumentException` with a specific message. This
  mirrors the up-front envelope validation in the webhook controller. Without it, a null field
  would surface as an opaque `NullPointerException` — which is *not* in the non-retryable set — so
  a single malformed message would burn the entire retry budget before dead-lettering with an
  unhelpful cause. Failing fast dead-letters promptly with a clear reason.
- **DLQ triage:** the recoverer unwraps the listener wrapper to the root cause and logs its type +
  message (e.g. `IllegalArgumentException: Transcript event missing data`) alongside the
  topic/partition/offset, so dead-lettered records are diagnosable from logs alone.

---

## Security

- **HMAC-SHA256** over the raw request body, compared in constant time (`MessageDigest.isEqual`).
- **Fail-closed:** when verification is enabled, a missing or invalid signature returns 401.
  Verification is toggleable (`app.webhook.signature-verification-enabled`) and disabled in the
  `local`/`test` profiles so the provided simulation script (which sends no signature) works.
- The HMAC secret is externalized via config/env, never hard-coded.

---

## Transcript Storage & Reconstruction

- When a session ends, `MeetingService` publishes a task to `transcript.reconstruct` (keyed by
  sessionId). `TranscriptReconstructConsumer` picks it up and delegates to
  `TranscriptReconstructionService`, which loads segments in `sequenceNumber` order, formats them,
  stores the result, and records the URI on the session.
- **Format:** one line per segment — `[start-end s] Speaker: content` — plain, human-readable text.
- **StorageService abstraction:** `store`/`retrieve` are behind an interface. `LocalFileStorageService`
  writes `{basePath}/{sessionId}.txt`. Swapping in S3/GCS is a new implementation, no change to
  reconstruction logic. The URI (not the blob) is what's persisted on the session, so the read API
  can fetch on demand.
- **Reconstruction as its own event/consumer** (rather than inline in the `meeting.ended` handler)
  keeps the DB write for ending a session fast, isolates the file I/O, and lets reconstruction retry
  independently. Failures flow through the same `DefaultErrorHandler` to `transcript.reconstruct.DLT`.
- **Empty transcript decision:** if a session ends with zero segments, an empty transcript file is
  still written (rather than failing/looping). This is a terminal, deterministic outcome; a meeting
  legitimately may have had no speech. If "completeness" (e.g. no sequence gaps) becomes a
  requirement, the validation hook lives in `TranscriptReconstructionService`.

---

## Read API

- **Endpoint:** `GET /api/v1/meetings/{meetingId}/sessions/{sessionId}/transcript`.
- **Ownership check:** the session must exist *and* belong to the given meeting
  (`findByIdAndMeetingId`); otherwise 404 with a meaningful message. This prevents leaking a valid
  session under the wrong meeting id.
- **Source of truth is the DB, not the file.** The structured `entries` are always loaded from
  `transcript_segments` ordered by `sequenceNumber`. This works uniformly whether the session is
  LIVE (still accumulating; no file yet) or ENDED (already reconstructed). The stored artifact's
  `transcriptUri` is included as a reference for consumers that want the assembled text file, but
  the API does not reverse-parse that file — round-tripping a denormalized artifact back into
  structured data would be fragile and redundant.
- **Response shape:** session metadata (meeting id/title, status, started/ended, transcriptUri,
  segment count) plus an ordered list of entries (sequenceNumber, speaker id/name, content,
  start/end offset seconds, language). Records/DTOs keep the domain entities out of the HTTP layer.
- **Pagination** is deferred — sessions hold a bounded number of segments at current scale. If
  needed, the ordered query already supports it via a `Pageable` overload.

---

## Observability

- **Correlation IDs:** `CorrelationIdFilter` mints/propagates `X-Correlation-Id` into the MDC and
  echoes it on responses; the producer copies it into a Kafka header; the consumer restores it to
  the MDC. This threads one id across HTTP → Kafka → processing for traceable logs.
- Actuator exposes health, info, metrics, and Prometheus; Grafana is provisioned via Docker Compose.
- (Custom Micrometer metrics + dashboard are Phase 8.)

---

## Testing Strategy

- **Unit tests** (Mockito) for service logic — fast, no Spring context.
- **Integration tests** (`@SpringBootTest` + `@EmbeddedKafka` + H2) for the HTTP→Kafka→DB flow
  and DLQ behavior; async assertions use Awaitility.
- **Isolation caveat:** unlike `@DataJpaTest`, `@SpringBootTest` does **not** roll back between
  methods. Integration tests clean the relevant tables in `@BeforeEach` and use unique UUIDs.

---

## Key Assumptions

1. `sessionId` is globally unique and is the correct partition/idempotency key.
2. A `meeting.started` normally precedes its transcripts and end; retries cover the rare
   out-of-order case, and a truly orphaned event dead-letters.
3. The webhook forwards raw JSON to Kafka; downstream owns schema interpretation.
4. Local dev may run without signature verification; production enables it.
5. An embedded/local database and in-process Kafka are acceptable for the assignment; the design
   documents how to scale out rather than deploying it.

---

## Deferred / Future Work

- Custom metrics + Grafana dashboard (Phase 8).
- Broader edge-case suite: out-of-order, late transcript after end, concurrent sessions (Phase 9).
- README, architecture diagram, final polish (Phase 10).
- Production hardening: external secret management, schema registry for typed events,
  consumer concurrency tuning, transactional outbox if the DB write and publish must be atomic.
