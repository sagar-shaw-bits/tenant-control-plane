#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "error: Docker is required to run integration tests" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "error: Docker is installed but the Docker daemon is not running" >&2
  exit 1
fi

postgres_name="tenant-control-plane-test-postgres-$$"
rabbitmq_name="tenant-control-plane-test-rabbitmq-$$"
cleanup() {
  docker rm -f "$postgres_name" "$rabbitmq_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "$postgres_name" \
  -e POSTGRES_USER=tenant \
  -e POSTGRES_PASSWORD=tenant \
  -e POSTGRES_DB=tenant_control_plane \
  -p 127.0.0.1::5432 postgres:16 >/dev/null
docker run -d --name "$rabbitmq_name" \
  -e RABBITMQ_DEFAULT_USER=tenant \
  -e RABBITMQ_DEFAULT_PASS=tenant \
  -p 127.0.0.1::5672 rabbitmq:3.13-management >/dev/null

for _ in $(seq 1 90); do
  postgres_ready=false
  rabbitmq_ready=false
  if docker exec "$postgres_name" pg_isready -U tenant -d tenant_control_plane >/dev/null 2>&1; then
    postgres_ready=true
  fi
  if docker exec --user rabbitmq "$rabbitmq_name" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    rabbitmq_ready=true
  fi
  if "$postgres_ready" && "$rabbitmq_ready"; then
    break
  fi
  sleep 1
done

if ! "$postgres_ready" || ! "$rabbitmq_ready"; then
  echo "error: test dependencies did not become ready" >&2
  docker logs "$postgres_name" >&2 || true
  docker logs "$rabbitmq_name" >&2 || true
  exit 1
fi

postgres_port="$(docker port "$postgres_name" 5432/tcp | awk -F: 'END {print $NF}')"
rabbitmq_port="$(docker port "$rabbitmq_name" 5672/tcp | awk -F: 'END {print $NF}')"
export TEST_DATABASE_URL="postgres://tenant:tenant@127.0.0.1:${postgres_port}/tenant_control_plane?sslmode=disable"
export TEST_RABBITMQ_URL="amqp://tenant:tenant@127.0.0.1:${rabbitmq_port}/"

go test ./...
