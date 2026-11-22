#!/bin/sh
# Publish a raw task update straight onto the broker with Redpanda's own CLI (rpk),
# bypassing the worker. Use it to reproduce duplicates, out-of-order and poison messages.
#
#   scripts/publish-update.sh <task_id> <in_progress|done|failed> [message_id]
#   scripts/publish-update.sh --raw '<any payload, e.g. not json>'
#
# Inspect what was dead-lettered:
#   docker compose exec redpanda rpk topic consume tenant.task-updates.dlq -n 1
set -eu

TOPIC=tenant.task-updates

if [ "${1:-}" = "--raw" ]; then
  printf '%s\n' "$2" | docker compose exec -T redpanda rpk topic produce "$TOPIC" -k poison
  exit 0
fi

TASK_ID=${1:?task_id required}
STATUS=${2:?status required (in_progress|done|failed)}
MESSAGE_ID=${3:-"$TASK_ID:$STATUS"}
NOW=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)

PAYLOAD=$(printf '{"schema_version":1,"message_id":"%s","task_id":"%s","status":"%s","occurred_at":"%s"}' \
  "$MESSAGE_ID" "$TASK_ID" "$STATUS" "$NOW")

echo "publishing to $TOPIC: $PAYLOAD"
printf '%s\n' "$PAYLOAD" | docker compose exec -T redpanda rpk topic produce "$TOPIC" -k "$TASK_ID"
