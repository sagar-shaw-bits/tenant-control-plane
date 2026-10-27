#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8080}"
if ! curl --fail --silent "${base_url}/readyz" >/dev/null; then
  echo "error: service is not ready at ${base_url}; start it with docker compose up --build" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required to parse API responses" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

slug="race-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
create_response="$(curl --fail-with-body --silent --show-error \
  -H 'content-type: application/json' \
  --data "{\"slug\":\"${slug}\",\"name\":\"race test\"}" \
  "${base_url}/v1/tenants")"
tenant_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["tenant"]["id"])' <<<"$create_response")"

for _ in $(seq 1 90); do
  tenant_response="$(curl --fail --silent "${base_url}/v1/tenants/${tenant_id}")"
  tenant_status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$tenant_response")"
  if [[ "$tenant_status" == "active" ]]; then
    break
  fi
  if [[ "$tenant_status" == "failed" ]]; then
    echo "error: initial deploy task failed; retry with a worker configured with FAIL_RATE=0" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$tenant_status" != "active" ]]; then
  echo "error: tenant did not become active within 90 seconds (status: ${tenant_status})" >&2
  exit 1
fi
version="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])' <<<"$tenant_response")"

export BASE_URL="$base_url" tenant_id version tmpdir
seq 1 20 | xargs -P 20 -I '{}' bash -c '
  curl --silent --output "$tmpdir/patch-{}.body" --write-out "%{http_code}" \
    -X PATCH -H "content-type: application/json" \
    --data "{\"name\":\"race update {}\",\"version\":${version}}" \
    "${BASE_URL}/v1/tenants/${tenant_id}" >"$tmpdir/patch-{}.status"
'
patch_successes="$(grep -l '^202$' "$tmpdir"/patch-*.status 2>/dev/null | wc -l | tr -d ' ')"

post_slug="duplicate-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
export post_slug
seq 1 20 | xargs -P 20 -I '{}' bash -c '
  curl --silent --output "$tmpdir/post-{}.body" --write-out "%{http_code}" \
    -H "content-type: application/json" \
    --data "{\"slug\":\"${post_slug}\",\"name\":\"duplicate create\"}" \
    "${BASE_URL}/v1/tenants" >"$tmpdir/post-{}.status"
'
post_successes="$(grep -l '^201$' "$tmpdir"/post-*.status 2>/dev/null | wc -l | tr -d ' ')"

printf 'Concurrent PATCH requests (same version): %s/20 succeeded (expected 1)\n' "$patch_successes"
printf 'Concurrent POST requests (same slug): %s/20 succeeded (expected 1)\n' "$post_successes"
[[ "$patch_successes" == "1" && "$post_successes" == "1" ]]
