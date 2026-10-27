#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 TASK_ID STATUS [UPDATE_ID]" >&2
  exit 2
fi

task_id="$1"
status="$2"
update_id="${3:-$(python3 -c 'import uuid; print(uuid.uuid4())')}"
case "$status" in
  in_progress|done|failed) ;;
  *)
    echo "error: STATUS must be in_progress, done, or failed" >&2
    exit 2
    ;;
esac

management_url="${RABBITMQ_MANAGEMENT_URL:-http://localhost:15672}"
rabbitmq_user="${RABBITMQ_USER:-tenant}"
rabbitmq_password="${RABBITMQ_PASSWORD:-tenant}"
occurred_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
payload="$(python3 -c 'import json,sys; print(json.dumps({"update_id":sys.argv[1],"task_id":sys.argv[2],"status":sys.argv[3],"occurred_at":sys.argv[4]}))' "$update_id" "$task_id" "$status" "$occurred_at")"
request="$(python3 -c 'import json,sys; print(json.dumps({"properties":{"delivery_mode":2,"message_id":sys.argv[1]},"routing_key":"tenant.update","payload":sys.argv[2],"payload_encoding":"string"}))' "$update_id" "$payload")"

curl --fail-with-body --silent --show-error \
  --user "${rabbitmq_user}:${rabbitmq_password}" \
  -H 'content-type: application/json' \
  --data "$request" \
  "${management_url}/api/exchanges/%2f/tenant.updates/publish"
printf '\n'
