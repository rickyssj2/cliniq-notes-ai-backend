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
- [x] Kafka UI shows message in `meeting.events` with correct key
- [x] Correlation ID present in logs
- [x] Unit tests: validation, HMAC logic
- [x] Integration test: HTTP → Kafka
- [x] Manual testing UI served at `/webhook-tester.html` (static page for posting sample payloads)

---

## Phase 4: Kafka Consumer + Meeting/Session Processing

**Objective:** Consume events, handle `meeting.started` and `meeting.ended`.

### Implementation
- [x] `MeetingEventConsumer` with `@KafkaListener` on `meeting.events`
- [x] Route by `eventType` header → `MeetingService`
- [x] `meeting.started`: upsert Meeting, create Session (LIVE)
- [x] `meeting.ended`: set status ENDED + `endedAt`, publish reconstruct task
- [x] Idempotency: skip if already applied
- [x] `DefaultErrorHandler` with backoff (configurable; prod 1s/5s/30s) + `DeadLetterPublishingRecoverer`

### Validation
- [x] `meeting.started` → Meeting + Session rows created
- [x] Duplicate `meeting.started` → no error, no duplicate rows
- [x] `meeting.ended` → status ENDED in DB
- [x] `meeting.ended` for unknown session → DLQ after retries
- [x] Reconstruct task visible in `transcript.reconstruct`
- [x] Unit + integration tests pass

---

## Phase 5: Transcript Segment Processing

**Objective:** Persist transcript segments idempotently with dedup.

### Implementation
- [x] Route `meeting.transcript` → `TranscriptService`
- [x] Upsert segment; on `transcript_id` conflict, log and skip (dedup + unique-constraint backstop)
- [x] Store speaker (id + name), content, sequenceNumber, offsets, language
- [x] Session existence check; retry (throw) if session not yet created

### Validation
- [x] started → transcript → segment in DB with correct data
- [x] Same transcript twice → one row, no error
- [x] 3 chunks with different sequence numbers → all stored, ordered
- [x] Transcript for non-existent session → retried (unit test; DLQ path shared with Phase 4)
- [x] Unit + integration tests pass

---

## Phase 6: Transcript Reconstruction + Storage Service

**Objective:** Assemble ordered segments into a transcript file via `StorageService`.

### Implementation
- [x] `StorageService` interface: `store(sessionId, content)`, `retrieve(sessionId)`
- [x] `LocalFileStorageService` → `./data/transcripts/{sessionId}.txt`
- [x] `TranscriptReconstructConsumer` on `transcript.reconstruct`
- [x] Query segments ordered, assemble, store (offsets now integer seconds)
- [x] Reuse shared `DefaultErrorHandler` backoff + DLQ → `transcript.reconstruct.DLT`
      (simpler than a bespoke re-queue/retry-count header; consistent across consumers)
- [x] Persist transcript URI on session

### Validation
- [x] Happy path → file at `{basePath}/{sessionId}.txt`
- [x] File content correctly ordered (speaker + content)
- [x] End with 0 chunks → empty transcript stored (documented decision; see DESIGN.md)
- [x] Unit tests: ordering, formatting, StorageService mock, OffsetParser
- [x] Integration test: full lifecycle → file written + URI persisted

---

## Phase 7: Read API — GET Transcript Endpoint

**Objective:** Retrieve full ordered transcript for a session.

### Implementation
- [ ] `GET /api/v1/meetings/{meetingId}/sessions/{sessionId}/transcript`
- [x] Structured entries always sourced from DB (ordered), uniform for LIVE + ENDED
- [x] Stored file `transcriptUri` referenced in the response (not reverse-parsed)
- [x] JSON response with session metadata + ordered entries
- [x] 404 for unknown meeting/session
- [ ] Optional pagination for segment view (deferred; not needed at current scale)

### Validation
- [x] Full simulation → GET returns complete transcript
- [x] Unknown session → 404 with meaningful message
- [x] Active (LIVE) session → segments from DB in order
- [x] Response includes speakers, content, offsets
- [x] Unit + integration tests pass

---

## Phase 8: Observability

**Objective:** Correlation IDs, Prometheus metrics, Grafana dashboard.

### Implementation
- [x] `CorrelationIdFilter`: mint/propagate UUID into MDC + Kafka headers (Phase 3)
- [x] Structured JSON logging via Boot's native `logging.structured.format` (opt-in `LOG_FORMAT`);
      `correlationId`, `sessionId`, `event` carried in MDC
- [x] Micrometer metrics: `webhook.events.received`, `consumer.event.processing.time`,
      `consumer.events.processed` (outcome-tagged), `transcript.reconstruction.count`,
      `kafka.consumer.dlq.count`
- [x] Expose `/actuator/prometheus` (Phase 1)
- [x] Prometheus scrape config (Phase 1)
- [x] Provisioned Grafana dashboard JSON

### Validation
- [x] Correlation ID consistent across controller → Kafka → consumer (test)
- [x] `/actuator/prometheus` shows custom metrics (test)
- [ ] Prometheus target UP (needs `docker compose up`)
- [ ] Grafana dashboard shows live data after simulation (needs `docker compose up`)

---

## Phase 9: Edge Cases + Test Scenarios

**Objective:** Harden behavior and cover scenarios beyond the happy path.

### Implementation
- [x] Duplicate transcript chunks (Phase 5 tests)
- [x] Out-of-order delivery (Phase 5 tests)
- [x] Transcript arriving after `meeting.ended` (EdgeCaseIntegrationTest)
- [x] `meeting.ended` without `meeting.started` (DeadLetterIntegrationTest)
- [x] Concurrent sessions for the same meeting (EdgeCaseIntegrationTest)
- [x] Malformed payloads (MalformedTranscriptDlqIntegrationTest + webhook 400 tests)
- [x] Invalid HMAC signature (WebhookSignatureIntegrationTest)
- [x] Consumer failure → retries → DLQ (DeadLetterIntegrationTest)
- [x] Documented each decision in DESIGN.md "Edge Case Behavior" (folded into README in Phase 10)

### Validation
- [x] Duplicate → one segment
- [x] Out-of-order → GET returns correct order
- [x] Late transcript → stored, behavior documented
- [x] Ended-without-started → DLQ after retries (documented)
- [x] Concurrent sessions tracked independently
- [x] DLQ populated after simulated failure
- [x] `./mvnw test` green (61 tests)
- [x] Simulation scripts: `scripts/simulate_meeting.sh` (happy path) + `scripts/simulate_edge_cases.sh`

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
