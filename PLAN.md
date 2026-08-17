# Implementation Plan — Event-Driven Meeting Webhook Service

## Status Legend
- [ ] Not started
- [x] Complete

---

## Phase 1: Project Skeleton + Docker Compose Infrastructure

**Objective:** Spring Boot project with modular package boundaries, build tooling, and all infra running in Docker Compose.

### Tech Baseline
- Spring Boot 4.1.0 (Spring Framework 7, Java 17 baseline)
- Maven + Maven Wrapper (`./mvnw`)
- Lombok 1.18.46 (boilerplate reduction on entities/DTOs)

### Implementation
- [x] Initialize Spring Boot 4.x project, Maven (with Maven Wrapper), Java 17+
- [x] Dependencies: webmvc, spring-boot-starter-kafka, data-jpa, validation, postgresql, flyway, actuator, micrometer-registry-prometheus, lombok
- [x] Package structure: `webhook`, `meeting`, `transcript`, `storage`, `common`
- [x] Docker Compose: Kafka (KRaft), PostgreSQL 16, kafka-ui, Prometheus, Grafana, app
- [x] Profiles: `local` (Docker infra), `test` (Testcontainers)
- [x] Flyway baseline migration

### Validation
- [ ] `./mvnw clean verify` succeeds (Lombok annotation processing works on the local JDK)
- [ ] `docker-compose up` starts all services without errors
- [ ] App connects to Kafka + PostgreSQL (visible in logs)
- [ ] `GET /actuator/health` returns `UP`
- [ ] Kafka UI reachable
- [ ] Prometheus `/targets` shows app UP
- [ ] Grafana reachable

---

## Phase 2: Database Schema + Domain Entities

**Objective:** JPA entities, repositories, and Flyway migrations for Meeting, Session, TranscriptSegment.

### Implementation
- [x] `V2__create_schema.sql`: meetings, sessions, transcript_segments
- [x] Unique constraint on `transcript_segments.transcript_id`
- [x] Index on `transcript_segments(session_id, sequence_number)`
- [x] JPA entities with Lombok (`@Getter`, `@Setter`, `@NoArgsConstructor`, `@Builder`;
      no `@Data`/`@EqualsAndHashCode` on entities): Meeting 1→* Session, Session 1→* TranscriptSegment
- [x] Repositories with `findBySessionIdOrderBySequenceNumber`
- [x] `SessionStatus` enum (LIVE, ENDED)

### Validation
- [x] Flyway creates all tables on startup
- [x] Table structure verified in psql
- [x] Duplicate `transcript_id` insert rejected by constraint
- [x] Repository query tests pass

---

## Phase 3: Kafka Producer + Webhook Endpoint

**Objective:** Validate payloads, verify HMAC, publish to Kafka, return 202.

### Implementation
- [x] `WebhookController` at `POST /api/v1/webhooks`
- [x] Request DTOs per event type with Jakarta validation
- [x] HMAC-SHA256 signature verification (configurable secret, fail-closed)
- [x] `KafkaProducerService` → `meeting.events`, key = sessionId
- [x] Correlation ID minted at controller (filter), added to Kafka headers + MDC
- [x] 202 / 400 / 401 response handling

### Validation
- [x] Valid `meeting.started` → 202
- [x] Missing signature → 401
- [x] Malformed JSON → 400 with meaningful message
- [ ] Kafka UI shows message in `meeting.events` with correct key
- [ ] Correlation ID present in logs
- [x] Unit tests: validation, HMAC logic
- [x] Integration test: HTTP → Kafka

---

## Phase 4: Kafka Consumer + Meeting/Session Processing

**Objective:** Consume events, handle `meeting.started` and `meeting.ended`.

### Implementation
- [ ] `MeetingEventConsumer` with `@KafkaListener` on `meeting.events`
- [ ] Route by `event` field → `MeetingService`
- [ ] `meeting.started`: upsert Meeting, create Session (LIVE)
- [ ] `meeting.ended`: set status ENDED + `endedAt`, publish reconstruct task
- [ ] Idempotency: skip if already applied
- [ ] `DefaultErrorHandler` with backoff (1s, 5s, 30s) + `DeadLetterPublishingRecoverer`

### Validation
- [ ] `meeting.started` → Meeting + Session rows created
- [ ] Duplicate `meeting.started` → no error, no duplicate rows
- [ ] `meeting.ended` → status ENDED in DB
- [ ] `meeting.ended` for unknown session → DLQ after retries
- [ ] Reconstruct task visible in `transcript.reconstruct`
- [ ] Unit + integration tests pass

---

## Phase 5: Transcript Segment Processing

**Objective:** Persist transcript segments idempotently with dedup.

### Implementation
- [ ] Route `meeting.transcript` → `TranscriptService`
- [ ] Upsert segment; on `transcript_id` conflict, log and skip
- [ ] Store speaker (id + name), content, sequenceNumber, offsets, language
- [ ] Session existence check; retry if session not yet created

### Validation
- [ ] started → transcript → segment in DB with correct data
- [ ] Same transcript twice → one row, no error
- [ ] 3 chunks with different sequence numbers → all stored, ordered
- [ ] Transcript for non-existent session → retried
- [ ] Unit + integration tests pass

---

## Phase 6: Transcript Reconstruction + Storage Service

**Objective:** Assemble ordered segments into a transcript file via `StorageService`.

### Implementation
- [ ] `StorageService` interface: `store(sessionId, content)`, `retrieve(sessionId)`
- [ ] `LocalFileStorageService` → `./data/transcripts/{sessionId}.txt`
- [ ] `TranscriptReconstructConsumer` on `transcript.reconstruct`
- [ ] Query segments ordered, validate completeness, assemble, store
- [ ] Re-queue with retry count header; backoff 5s / 15s / 60s; DLQ after max
- [ ] Persist transcript URI on session

### Validation
- [ ] Happy path → file at `./data/transcripts/{sessionId}.txt`
- [ ] File content correctly ordered (speaker + content)
- [ ] End with 0 chunks → retries observable, then partial/DLQ
- [ ] Unit tests: ordering, formatting, StorageService mock
- [ ] Integration test: Kafka → file written

---

## Phase 7: Read API — GET Transcript Endpoint

**Objective:** Retrieve full ordered transcript for a session.

### Implementation
- [ ] `GET /api/v1/meetings/{meetingId}/sessions/{sessionId}/transcript`
- [ ] Fast path: assembled transcript from storage
- [ ] Fallback: order segments from DB on the fly
- [ ] JSON response with session metadata + ordered entries
- [ ] 404 for unknown meeting/session
- [ ] Optional pagination for segment view

### Validation
- [ ] Full simulation → GET returns complete transcript
- [ ] Unknown session → 404 with meaningful message
- [ ] Active session (no file yet) → segments from DB in order
- [ ] Response includes speakers, content, offsets
- [ ] Unit + integration tests pass

---

## Phase 8: Observability

**Objective:** Correlation IDs, Prometheus metrics, Grafana dashboard.

### Implementation
- [ ] `CorrelationIdFilter`: mint/propagate UUID into MDC + Kafka headers
- [ ] Logback JSON encoder with `correlationId`, `sessionId`, `event`
- [ ] Micrometer metrics: `webhook.events.received`, processing timer,
      `transcript.reconstruction.count`, `kafka.consumer.retry.count`
- [ ] Expose `/actuator/prometheus`
- [ ] Prometheus scrape config
- [ ] Provisioned Grafana dashboard JSON

### Validation
- [ ] Correlation ID consistent across controller → Kafka → consumer
- [ ] `/actuator/prometheus` shows custom metrics
- [ ] Prometheus target UP
- [ ] Grafana dashboard shows live data after simulation

---

## Phase 9: Edge Cases + Test Scenarios

**Objective:** Harden behavior and cover scenarios beyond the happy path.

### Implementation
- [ ] Duplicate transcript chunks
- [ ] Out-of-order delivery
- [ ] Transcript arriving after `meeting.ended`
- [ ] `meeting.ended` without `meeting.started`
- [ ] Concurrent sessions for the same meeting
- [ ] Malformed payloads
- [ ] Invalid HMAC signature
- [ ] Consumer failure → retries → DLQ
- [ ] Document each decision in README

### Validation
- [ ] Duplicate script → one segment
- [ ] Out-of-order script → GET returns correct order
- [ ] Late transcript → stored, behavior documented
- [ ] Ended-without-started → documented outcome (DLQ or ENDED session)
- [ ] Concurrent sessions tracked independently
- [ ] DLQ populated after simulated failure
- [ ] `./mvnw test` green

---

## Phase 10: Documentation + Final Polish

**Objective:** README, architecture doc, submission-ready repo.

### Implementation
- [ ] README: quick start, architecture diagram, trade-offs, future work, test instructions
- [ ] Formatting cleanup, remove dead code, Javadoc on public APIs
- [ ] `./mvnw clean verify` green
- [ ] Simulation scripts committed (provided + custom edge cases)

### Validation
- [ ] Fresh clone → `docker-compose up` → healthy
- [ ] Simulation script → all 202s
- [ ] GET transcript correct
- [ ] `./mvnw test` passes
- [ ] README complete
- [ ] Kafka UI shows topics + DLQ
- [ ] Grafana shows simulation metrics
