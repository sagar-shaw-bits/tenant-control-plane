# Reviewer validation guide

This runbook gives a repeatable demonstration of the tenant provisioning
control plane and maps its checks to the take-home requirements. Run the Bash
steps from the repository root in Git Bash, WSL, or another Bash environment.

## Prerequisites

- Go 1.27.3 or later
- Docker Desktop or Docker Engine, with Docker Compose and a running daemon
- Bash, `curl`, `jq`, and Python 3 (Python 3 is used by the race and publish
  helper scripts)
- For `./scripts/check.sh`: `goimports`, `golangci-lint` v2, and `govulncheck`

The Compose stack uses local-development credentials (`tenant` / `tenant`).
Do not use these defaults outside a local demo.

## 1. Start a clean demo stack

If a stack is already running, stop its containers without deleting its
database volume:

```bash
docker compose down
```

Set a longer worker delay so the asynchronous states are easy to observe, then
boot the API, worker, PostgreSQL, and RabbitMQ together:

```bash
export MIN_DELAY_MS=5000
export MAX_DELAY_MS=5000
docker compose up --build -d

until curl -fsS http://localhost:8080/readyz >/dev/null; do sleep 1; done
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
```

Both health endpoints should return HTTP `200`. The readiness endpoint checks
PostgreSQL and RabbitMQ; liveness only reports that the HTTP process is alive.

Define small helpers for checking asynchronous results and structured errors:

```bash
BASE=http://localhost:8080

wait_for_status() {
  url="$1"
  expected="$2"
  for _ in $(seq 1 80); do
    body="$(curl -fsS "$url")" || return 1
    status="$(jq -r '.status' <<<"$body")"
    if [[ "$status" == "$expected" ]]; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 0.25
  done
  echo "Timed out waiting for $url to reach $expected" >&2
  curl -fsS "$url" | jq . >&2 || true
  return 1
}

expect_error() {
  expected_status="$1"
  expected_code="$2"
  shift 2
  response="$(curl -sS -w $'\n%{http_code}' "$@")" || return 1
  actual_status="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$actual_status" != "$expected_status" ]]; then
    echo "Expected HTTP $expected_status, received $actual_status: $body" >&2
    return 1
  fi
  if ! jq -e --arg code "$expected_code" '.error.code == $code' <<<"$body" >/dev/null; then
    echo "Expected error code $expected_code: $body" >&2
    return 1
  fi
  echo "PASS: HTTP $actual_status, error=$expected_code"
}

create_tenant() {
  local tenant_slug="$1"
  local tenant_name="$2"
  curl -fsS -X POST "$BASE/v1/tenants" \
    -H 'Content-Type: application/json' \
    -d "{\"slug\":\"$tenant_slug\",\"name\":\"$tenant_name\"}"
}
```

## 2. Create and observe the deploy lifecycle

Create a tenant with a unique, valid slug:

```bash
slug="demo-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
created="$(curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$slug\",\"name\":\"Demo tenant\"}")"
echo "$created" | jq .

tenant_id="$(jq -r '.tenant.id' <<<"$created")"
deploy_task_id="$(jq -r '.task.id' <<<"$created")"
```

Expected: HTTP `201`; tenant status `provisioning`, version `1`; task type
`deploy`, status `accepted`. Check the response shape, UUIDs, and UTC
millisecond timestamps:

```bash
jq -e '
  .tenant.status == "provisioning" and
  .tenant.version == 1 and
  .task.type == "deploy" and
  .task.status == "accepted" and
  (.tenant.id | test("^[0-9a-f-]{36}$")) and
  (.task.id | test("^[0-9a-f-]{36}$")) and
  (.tenant.created_at | test("\\.[0-9]{3}Z$")) and
  (.tenant.updated_at | test("\\.[0-9]{3}Z$")) and
  (.task.created_at | test("\\.[0-9]{3}Z$")) and
  (.task.updated_at | test("\\.[0-9]{3}Z$"))
' <<<"$created"
```

Poll for `in_progress` while the worker is simulating work, then verify the
terminal task and tenant states:

```bash
wait_for_status "$BASE/v1/tasks/$deploy_task_id" in_progress | jq .
wait_for_status "$BASE/v1/tasks/$deploy_task_id" done | jq .
wait_for_status "$BASE/v1/tenants/$tenant_id" active | jq .
active_tenant="$(curl -fsS "$BASE/v1/tenants/$tenant_id")"
test "$(jq -r '.version' <<<"$active_tenant")" -eq 2
```

Expected transitions: task `accepted → in_progress → done`; tenant
`provisioning → active`; the tenant version is now `2` after the
worker-applied terminal mutation.

## 3. Check REST reads, filtering, and pagination

```bash
curl -fsS "$BASE/v1/tenants/$tenant_id" | jq .
curl -fsS "$BASE/v1/tenants?status=active&limit=1" | jq .
curl -fsS "$BASE/v1/tenants/$tenant_id/tasks?limit=1" | jq .
curl -fsS "$BASE/v1/tasks?tenant_id=$tenant_id&status=done&limit=1" | jq .
curl -fsS "$BASE/v1/tasks/$deploy_task_id" | jq .
```

List responses contain `items`; they include `next_cursor` only when another
page exists. Create at least two tenants to demonstrate the next-page path:

```bash
for n in 1 2; do
  page_slug="page-$n-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
  curl -fsS -X POST "$BASE/v1/tenants" \
    -H 'Content-Type: application/json' \
    -d "{\"slug\":\"$page_slug\",\"name\":\"Pagination $n\"}" >/dev/null
done

first_page="$(curl -fsS "$BASE/v1/tenants?limit=1")"
echo "$first_page" | jq .
cursor="$(jq -r '.next_cursor // empty' <<<"$first_page")"
test -n "$cursor"
curl -fsS --get "$BASE/v1/tenants" \
  --data-urlencode 'limit=1' \
  --data-urlencode "cursor=$cursor" | jq .
```

## 4. Verify state guards, validation, and error contracts

The following checks expect both the listed HTTP status and JSON error code:

```bash
# Invalid slug and blank name.
expect_error 400 validation_error \
  -X POST "$BASE/v1/tenants" -H 'Content-Type: application/json' \
  -d '{"slug":"Bad_slug","name":"Invalid"}'
expect_error 400 validation_error \
  -X POST "$BASE/v1/tenants" -H 'Content-Type: application/json' \
  -d '{"slug":"valid-slug","name":"  "}'

# Existing slug remains reserved, including after destruction.
expect_error 409 tenant_already_exists \
  -X POST "$BASE/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$slug\",\"name\":\"Duplicate\"}"

# Unknown tenant and task use valid but nonexistent UUIDs.
missing_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
expect_error 404 tenant_not_found "$BASE/v1/tenants/$missing_id"
expect_error 404 task_not_found "$BASE/v1/tasks/$missing_id"

# Bad pagination query.
expect_error 400 validation_error "$BASE/v1/tenants?limit=abc"

# Task HTTP API is read-only.
expect_error 405 method_not_allowed \
  -X PUT "$BASE/v1/tasks/$deploy_task_id"
```

To see state guards while a tenant is not active, stop the worker, create
another tenant, and try to mutate it while it is still provisioning:

```bash
docker compose stop worker
guard_slug="guard-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:10])')"
guard_create="$(curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$guard_slug\",\"name\":\"Guard test\"}")"
guard_id="$(jq -r '.tenant.id' <<<"$guard_create")"

expect_error 409 tenant_update_not_allowed \
  -X PATCH "$BASE/v1/tenants/$guard_id" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Not allowed yet","version":1}'
expect_error 409 tenant_update_not_allowed \
  -X DELETE "$BASE/v1/tenants/$guard_id"
docker compose start worker
```

## 5. Verify update, version conflicts, and destroy

Read the current version and submit one valid PATCH:

```bash
current_version="$(curl -fsS "$BASE/v1/tenants/$tenant_id" | jq -r '.version')"
old_version="$current_version"
update="$(curl -fsS -X PATCH "$BASE/v1/tenants/$tenant_id" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Updated demo tenant\",\"version\":$current_version}")"
echo "$update" | jq .
update_task_id="$(jq -r '.task.id' <<<"$update")"
updating_version="$(jq -r '.tenant.version' <<<"$update")"
test "$updating_version" -eq 3
```

Expected: HTTP `202`; tenant `updating`; task type `update`, status `accepted`.
While this task is open, a second update and a delete should be rejected:

```bash
expect_error 409 tenant_update_not_allowed \
  -X PATCH "$BASE/v1/tenants/$tenant_id" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Concurrent update\",\"version\":$updating_version}"
expect_error 409 tenant_update_not_allowed \
  -X DELETE "$BASE/v1/tenants/$tenant_id"
```

After the worker completes the update, test stale-version rejection:

```bash
wait_for_status "$BASE/v1/tasks/$update_task_id" done | jq .
wait_for_status "$BASE/v1/tenants/$tenant_id" active | jq .
updated_tenant="$(curl -fsS "$BASE/v1/tenants/$tenant_id")"
test "$(jq -r '.version' <<<"$updated_tenant")" -eq 4
test "$(jq -r '.name' <<<"$updated_tenant")" = 'Updated demo tenant'
expect_error 409 tenant_version_conflict \
  -X PATCH "$BASE/v1/tenants/$tenant_id" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Stale version\",\"version\":$old_version}"
```

Delete the active tenant, then verify terminal state and continued slug
uniqueness:

```bash
deleted="$(curl -fsS -X DELETE "$BASE/v1/tenants/$tenant_id")"
echo "$deleted" | jq .
destroy_task_id="$(jq -r '.task.id' <<<"$deleted")"
test "$(jq -r '.tenant.version' <<<"$deleted")" -eq 5
wait_for_status "$BASE/v1/tasks/$destroy_task_id" done | jq .
destroyed_tenant="$(wait_for_status "$BASE/v1/tenants/$tenant_id" destroyed)"
test "$(jq -r '.version' <<<"$destroyed_tenant")" -eq 6
echo "$destroyed_tenant" | jq .
expect_error 409 tenant_already_exists \
  -X POST "$BASE/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$slug\",\"name\":\"Still reserved\"}"
```

## 6. Verify worker failure and recovery

Recreate only the worker with failure forced on. This configuration is read at
worker startup:

```bash
FAIL_RATE=1 MIN_DELAY_MS=100 MAX_DELAY_MS=200 \
  docker compose up -d --no-deps --force-recreate worker
```

Create a fresh tenant and wait for its deploy task and tenant to fail:

```bash
failure_slug="fail-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:10])')"
failure_create="$(curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$failure_slug\",\"name\":\"Failure test\"}")"
failure_tenant_id="$(jq -r '.tenant.id' <<<"$failure_create")"
failure_task_id="$(jq -r '.task.id' <<<"$failure_create")"
wait_for_status "$BASE/v1/tasks/$failure_task_id" failed | jq .
wait_for_status "$BASE/v1/tenants/$failure_tenant_id" failed | jq .
```

A failed tenant may be deleted. Under `FAIL_RATE=1`, that destroy attempt also
fails and returns the tenant to `failed`:

```bash
failed_delete="$(curl -fsS -X DELETE "$BASE/v1/tenants/$failure_tenant_id")"
failed_destroy_id="$(jq -r '.task.id' <<<"$failed_delete")"
wait_for_status "$BASE/v1/tasks/$failed_destroy_id" failed | jq .
wait_for_status "$BASE/v1/tenants/$failure_tenant_id" failed | jq .
```

To also verify the `updating → failed` transition, first start a successful
worker and create a fresh tenant:

```bash
FAIL_RATE=0 MIN_DELAY_MS=100 MAX_DELAY_MS=200 \
  docker compose up -d --no-deps --force-recreate worker
update_failure_slug="update-fail-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
update_failure_create="$(curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$update_failure_slug\",\"name\":\"Update failure test\"}")"
update_failure_tenant_id="$(jq -r '.tenant.id' <<<"$update_failure_create")"
update_failure_deploy_id="$(jq -r '.task.id' <<<"$update_failure_create")"
wait_for_status "$BASE/v1/tasks/$update_failure_deploy_id" done >/dev/null
wait_for_status "$BASE/v1/tenants/$update_failure_tenant_id" active >/dev/null
```

Now force that tenant's update to fail:

```bash
FAIL_RATE=1 MIN_DELAY_MS=100 MAX_DELAY_MS=200 \
  docker compose up -d --no-deps --force-recreate worker
update_failure_version="$(curl -fsS "$BASE/v1/tenants/$update_failure_tenant_id" |
  jq -r '.version')"
update_failure="$(curl -fsS -X PATCH "$BASE/v1/tenants/$update_failure_tenant_id" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"This update fails\",\"version\":$update_failure_version}")"
update_failure_task_id="$(jq -r '.task.id' <<<"$update_failure")"
wait_for_status "$BASE/v1/tasks/$update_failure_task_id" failed | jq .
wait_for_status "$BASE/v1/tenants/$update_failure_tenant_id" failed | jq .
```

Restore a successful worker and delete the failed deploy tenant:

```bash
FAIL_RATE=0 MIN_DELAY_MS=100 MAX_DELAY_MS=200 \
  docker compose up -d --no-deps --force-recreate worker
retry_delete="$(curl -fsS -X DELETE "$BASE/v1/tenants/$failure_tenant_id")"
retry_destroy_id="$(jq -r '.task.id' <<<"$retry_delete")"
wait_for_status "$BASE/v1/tasks/$retry_destroy_id" done | jq .
wait_for_status "$BASE/v1/tenants/$failure_tenant_id" destroyed | jq .
update_failure_delete="$(curl -fsS -X DELETE "$BASE/v1/tenants/$update_failure_tenant_id")"
update_failure_destroy_id="$(jq -r '.task.id' <<<"$update_failure_delete")"
wait_for_status "$BASE/v1/tasks/$update_failure_destroy_id" done | jq .
wait_for_status "$BASE/v1/tenants/$update_failure_tenant_id" destroyed | jq .
```

This demonstrates failures from `provisioning`, `updating`, and `destroying`,
plus `failed → destroying → destroyed`.

## 7. Verify duplicate and out-of-order worker updates

### Duplicate updates from the worker simulator

```bash
DUPLICATE_RATE=1 FAIL_RATE=0 OUT_OF_ORDER=false \
  docker compose up -d --no-deps --force-recreate worker
```

Create a fresh tenant and poll its task and tenant endpoints:

```bash
duplicate_slug="duplicate-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:10])')"
duplicate_create="$(create_tenant "$duplicate_slug" "Duplicate update test")"
duplicate_tenant_id="$(jq -r '.tenant.id' <<<"$duplicate_create")"
duplicate_task_id="$(jq -r '.task.id' <<<"$duplicate_create")"
wait_for_status "$BASE/v1/tasks/$duplicate_task_id" done | jq .
wait_for_status "$BASE/v1/tenants/$duplicate_tenant_id" active | jq .
```

The worker republishes each update with the same `update_id`; duplicate delivery
must not cause another state mutation.

### Out-of-order terminal result

```bash
DUPLICATE_RATE=0 FAIL_RATE=0 OUT_OF_ORDER=true \
  docker compose up -d --no-deps --force-recreate worker
```

Create another fresh tenant, then poll task and tenant endpoints. The worker
sends the terminal result before `in_progress`; expected final state is task
`done` and tenant `active`.

```bash
order_slug="order-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:10])')"
order_create="$(create_tenant "$order_slug" "Out-of-order success test")"
order_tenant_id="$(jq -r '.tenant.id' <<<"$order_create")"
order_task_id="$(jq -r '.task.id' <<<"$order_create")"
wait_for_status "$BASE/v1/tasks/$order_task_id" done | jq .
wait_for_status "$BASE/v1/tenants/$order_tenant_id" active | jq .
```

To also verify the out-of-order failed result, recreate the worker with both
settings:

```bash
OUT_OF_ORDER=true FAIL_RATE=1 \
  docker compose up -d --no-deps --force-recreate worker
```

Create a fresh tenant and poll it. Expected final state: task `failed`, tenant
`failed`.

```bash
order_fail_slug="order-fail-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
order_fail_create="$(create_tenant "$order_fail_slug" "Out-of-order failure test")"
order_fail_tenant_id="$(jq -r '.tenant.id' <<<"$order_fail_create")"
order_fail_task_id="$(jq -r '.task.id' <<<"$order_fail_create")"
wait_for_status "$BASE/v1/tasks/$order_fail_task_id" failed | jq .
wait_for_status "$BASE/v1/tenants/$order_fail_tenant_id" failed | jq .
```

### Manual duplicate, stale, and poison updates

Stop the worker before creating a fresh tenant. This prevents it from consuming
the task before the manual updates are published:

```bash
docker compose stop worker
manual_slug="manual-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:10])')"
manual_create="$(curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$manual_slug\",\"name\":\"Manual update test\"}")"
manual_task_id="$(jq -r '.task.id' <<<"$manual_create")"
manual_tenant_id="$(jq -r '.tenant.id' <<<"$manual_create")"
duplicate_update_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"

./scripts/publish-update.sh "$manual_task_id" in_progress
./scripts/publish-update.sh "$manual_task_id" done "$duplicate_update_id"
./scripts/publish-update.sh "$manual_task_id" done "$duplicate_update_id"
wait_for_status "$BASE/v1/tasks/$manual_task_id" done | jq .
wait_for_status "$BASE/v1/tenants/$manual_tenant_id" active | jq .
```

Then send a new, stale progress update to that terminal task. It must not
regress:

```bash
./scripts/publish-update.sh "$manual_task_id" in_progress
wait_for_status "$BASE/v1/tasks/$manual_task_id" done | jq .
docker compose start worker
```

To verify unknown-task poison handling, publish a valid update for a task UUID
that does not exist:

```bash
unknown_task_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
./scripts/publish-update.sh "$unknown_task_id" done
```

The update is rejected to RabbitMQ's `tenant.dead.queue`. Inspect it at
`http://localhost:15672` under **Queues**; local credentials default to
`tenant` / `tenant`. Malformed JSON can also be published through the RabbitMQ
management UI, using the `tenant.updates` exchange and routing key
`tenant.update`; it should be rejected to the same dead-letter queue.

Restore worker defaults after these scenarios:

```bash
docker compose stop worker
unset FAIL_RATE DUPLICATE_RATE OUT_OF_ORDER MIN_DELAY_MS MAX_DELAY_MS
docker compose up -d --no-deps worker
```

## 8. Verify concurrency and persistence

The repository race script makes 20 concurrent same-version PATCH requests and
20 concurrent same-slug create requests. It requires the API and worker to be
running with a successful worker:

```bash
FAIL_RATE=1 ./scripts/race.sh
```

Expected: **1/20** requests succeed in each group.

For persistence, retain the Postgres volume, restart the control plane, and
read a tenant created earlier:

```bash
docker compose restart control-plane
until curl -fsS "$BASE/readyz" >/dev/null; do sleep 1; done
curl -fsS "$BASE/v1/tenants/$tenant_id" | jq .
```

Expected: HTTP `200` and the tenant record is still present.

## 9. Run automated tests and quality gates

```bash
./scripts/test.sh
./scripts/check.sh
```

`scripts/test.sh` starts temporary PostgreSQL and RabbitMQ containers and runs
`go test ./...`, including database, broker, outbox, state-machine,
idempotency, ordering, and concurrency integration coverage. `scripts/check.sh`
runs Go formatting checks, `goimports`, `golangci-lint`, `govulncheck`, the test
script, and Gitleaks when installed.

## Cleanup

Stop containers while retaining local database data:

```bash
docker compose down
```

Remove the database volume only when you intentionally want to erase the
persisted demo data:

```bash
docker compose down -v
```
