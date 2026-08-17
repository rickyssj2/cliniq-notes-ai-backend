#!/bin/bash
# simulate_edge_cases.sh — Exercises edge cases beyond the happy path.
# Usage: ./simulate_edge_cases.sh [BASE_URL]
#
# Scenarios:
#   1. Duplicate transcript chunk (same transcriptId twice)  -> stored once
#   2. Out-of-order delivery (seq 3, 1, 2)                    -> GET returns 1,2,3
#   3. Transcript after meeting.ended                         -> still stored
#   4. meeting.ended without meeting.started                 -> retried then DLQ
#   5. Concurrent sessions for the same meeting               -> tracked independently
#   6. Malformed payload (missing data)                       -> 202 accepted, later DLQ
#
# Each webhook returns 202 immediately (async processing). Use the GET endpoint and the
# service logs / Kafka UI (meeting.events.DLT) to observe the outcomes.

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
WEBHOOK_URL="$BASE_URL/api/v1/webhooks"

uuid() { uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid; }

post() {
  curl -s -o /dev/null -w "  -> HTTP %{http_code}\n" \
    -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" -d "$1"
}

started() {
  post '{ "event": "meeting.started",
    "meeting": { "id": "'"$1"'", "sessionId": "'"$2"'", "title": "Edge Case",
      "startedAt": "2024-12-13T06:57:09.736Z", "createdAt": "2024-12-13T06:57:09.736Z" } }'
}

ended() {
  post '{ "event": "meeting.ended",
    "meeting": { "id": "'"$1"'", "sessionId": "'"$2"'", "endedAt": "2024-12-13T07:04:37.052Z" },
    "reason": "HOST_ENDED_MEETING" }'
}

# args: meetingId sessionId transcriptId seq content
transcript() {
  post '{ "event": "meeting.transcript",
    "meeting": { "id": "'"$1"'", "sessionId": "'"$2"'" },
    "data": { "transcriptId": "'"$3"'", "sequenceNumber": '"$4"',
      "speaker": { "id": "70c5d391-5bca-4cf3-9907-bec205798adb", "name": "Alice" },
      "content": "'"$5"'", "startOffset": "'"$4"'", "endOffset": "'"$4"'", "language": "en" } }'
}

echo "############################################################"
echo "# 1. Duplicate transcript chunk (stored once)"
echo "############################################################"
M=$(uuid); S=$(uuid); DUP=$(uuid)
started "$M" "$S"; sleep 1
transcript "$M" "$S" "$DUP" 1 "duplicate me"
transcript "$M" "$S" "$DUP" 1 "duplicate me"
echo "  Expect exactly ONE segment for session $S"
echo ""

echo "############################################################"
echo "# 2. Out-of-order delivery (seq 3, 1, 2 -> read back 1,2,3)"
echo "############################################################"
M=$(uuid); S=$(uuid)
started "$M" "$S"; sleep 1
transcript "$M" "$S" "$(uuid)" 3 "third"
transcript "$M" "$S" "$(uuid)" 1 "first"
transcript "$M" "$S" "$(uuid)" 2 "second"
sleep 1
echo "  GET returns entries ordered 1,2,3:"
echo "    curl -s $BASE_URL/api/v1/meetings/$M/sessions/$S/transcript | jq '.entries[].content'"
echo ""

echo "############################################################"
echo "# 3. Transcript arriving after meeting.ended (still stored)"
echo "############################################################"
M=$(uuid); S=$(uuid)
started "$M" "$S"; sleep 1
transcript "$M" "$S" "$(uuid)" 1 "during"
ended "$M" "$S"; sleep 1
transcript "$M" "$S" "$(uuid)" 2 "late arrival"
echo "  Expect 2 segments stored; session remains ENDED"
echo ""

echo "############################################################"
echo "# 4. meeting.ended without meeting.started (retry -> DLQ)"
echo "############################################################"
M=$(uuid); S=$(uuid)
ended "$M" "$S"
echo "  Expect retries then a record on meeting.events.DLT (check Kafka UI / logs)"
echo ""

echo "############################################################"
echo "# 5. Concurrent sessions for the same meeting"
echo "############################################################"
M=$(uuid); SA=$(uuid); SB=$(uuid)
started "$M" "$SA"
started "$M" "$SB"
transcript "$M" "$SA" "$(uuid)" 1 "session A line"
transcript "$M" "$SB" "$(uuid)" 1 "session B line"
ended "$M" "$SA"
echo "  Session A -> ENDED, Session B -> LIVE, segments attributed independently"
echo ""

echo "############################################################"
echo "# 6. Malformed payload (missing data) -> 202, later DLQ"
echo "############################################################"
M=$(uuid); S=$(uuid)
started "$M" "$S"; sleep 1
post '{ "event": "meeting.transcript", "meeting": { "id": "'"$M"'", "sessionId": "'"$S"'" } }'
echo "  Accepted at edge (202); fails fast in consumer -> meeting.events.DLT"
echo ""

echo "=== Edge-case simulation complete ==="
