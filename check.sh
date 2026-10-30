#!/usr/bin/env bash
# Quality gate for CI and build-lab: format, vet, tests.
set -euo pipefail
unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "gofmt needed:" >&2; echo "$unformatted" >&2; exit 1
fi
go vet ./...
if [[ -n "${CI:-}" ]]; then
  go test -race -count=1 ./...
else
  go test -count=1 ./...
fi
