#!/usr/bin/env bash
set -euo pipefail

mapfile -t go_files < <(find . -name '*.go' -not -path './vendor/*')
unformatted="$(gofmt -l "${go_files[@]}")"
if [[ -n "$unformatted" ]]; then
  printf 'Go files need gofmt:\n%s\n' "$unformatted" >&2
  exit 1
fi

if ! command -v goimports >/dev/null 2>&1; then
  echo "error: goimports is required; install with: go install golang.org/x/tools/cmd/goimports@latest" >&2
  exit 1
fi
unformatted_imports="$(goimports -l "${go_files[@]}")"
if [[ -n "$unformatted_imports" ]]; then
  printf 'Go files need goimports:\n%s\n' "$unformatted_imports" >&2
  exit 1
fi

if ! command -v golangci-lint >/dev/null 2>&1; then
  echo "error: golangci-lint is required: https://golangci-lint.run/welcome/install/" >&2
  exit 1
fi
golangci-lint run ./...

if ! command -v govulncheck >/dev/null 2>&1; then
  echo "error: govulncheck is required; install with: go install golang.org/x/vuln/cmd/govulncheck@latest" >&2
  exit 1
fi
govulncheck ./...

./scripts/test.sh

if command -v gitleaks >/dev/null 2>&1; then
  gitleaks detect --source . --no-git
fi
