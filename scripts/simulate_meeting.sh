#!/bin/bash
# simulate_meeting.sh — Happy-path simulation script
# Usage: ./simulate_meeting.sh [BASE_URL]
#
# Simulates a basic meeting lifecycle:
#   meeting.started -> transcript chunks -> meeting.ended
# then prints how to verify the reconstructed transcript.

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
WEBHOOK_URL="$BASE_URL/api/v1/webhooks"

MEETING_ID="50c8940e-1b97-402a-97d6-2708b7feca41"
SESSION_ID="05e57591-d89e-45c9-ae44-08dc1eaad0e0"
ORGANIZER_ID="70c5d391-5bca-4cf3-9907-bec205798adb"

post() {
  curl -s -o /dev/null -w "  -> HTTP %{http_code}\n" \
    -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" -d "$1"
}

echo "=== Sending meeting.started ==="
post '{
  "event": "meeting.started",
  "meeting": {
    "id": "'$MEETING_ID'",
    "sessionId": "'$SESSION_ID'",
    "title": "Q4 Planning Sync",
    "roomName": "lcfvaa-absxch",
    "status": "LIVE",
    "createdAt": "2024-12-13T06:57:09.736Z",
    "startedAt": "2024-12-13T06:57:09.736Z",
    "organizedBy": { "id": "'$ORGANIZER_ID'", "name": "Alice Johnson" }
  }
}'
sleep 1

SPEAKERS=("Alice Johnson" "Bob Smith" "Alice Johnson")
SPEAKER_IDS=(
  "70c5d391-5bca-4cf3-9907-bec205798adb"
  "82d6e402-6cdb-5df4-a018-19ed2fbce1bc"
  "70c5d391-5bca-4cf3-9907-bec205798adb"
)
CONTENTS=(
  "Alright, let us get started with the Q4 planning."
  "Sure. I have the revenue projections ready to share."
  "Great, go ahead and walk us through the numbers."
)
# Offsets are relative seconds from session start.
START_OFFSETS=("2" "6" "9")
END_OFFSETS=("5" "8" "12")

for i in 0 1 2; do
  SEQ=$((i + 1))
  TRANSCRIPT_ID=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
  echo "=== Sending transcript chunk #$SEQ ==="
  post '{
    "event": "meeting.transcript",
    "meeting": { "id": "'$MEETING_ID'", "sessionId": "'$SESSION_ID'" },
    "data": {
      "transcriptId": "'$TRANSCRIPT_ID'",
      "sequenceNumber": '$SEQ',
      "speaker": { "id": "'${SPEAKER_IDS[$i]}'", "name": "'"${SPEAKERS[$i]}"'" },
      "content": "'"${CONTENTS[$i]}"'",
      "startOffset": "'${START_OFFSETS[$i]}'",
      "endOffset": "'${END_OFFSETS[$i]}'",
      "language": "en"
    }
  }'
  sleep 1
done

echo "=== Sending meeting.ended ==="
post '{
  "event": "meeting.ended",
  "meeting": {
    "id": "'$MEETING_ID'",
    "sessionId": "'$SESSION_ID'",
    "title": "Q4 Planning Sync",
    "status": "LIVE",
    "createdAt": "2024-12-13T06:57:09.736Z",
    "startedAt": "2024-12-13T06:57:09.736Z",
    "endedAt": "2024-12-13T07:04:37.052Z",
    "organizedBy": { "id": "'$ORGANIZER_ID'", "name": "Alice Johnson" }
  },
  "reason": "HOST_ENDED_MEETING"
}'

echo ""
echo "=== Simulation complete ==="
echo "Verify the reconstructed transcript:"
echo "  curl -s $BASE_URL/api/v1/meetings/$MEETING_ID/sessions/$SESSION_ID/transcript | jq ."
