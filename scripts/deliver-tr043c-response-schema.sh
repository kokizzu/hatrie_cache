#!/usr/bin/env bash
set -euo pipefail

mode=${1:-inspect}
files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  Makefile
  TR043C_COMPACT_PEER_RESPONSE_SCHEMA.md
  hat/hatPeer/compact_listener.go
  hat/hatPeer/compact_protocol.go
  hat/hatPeer/compact_request_template.go
  hat/hatPeer/compact_session.go
  hat/hatPeer/tr043c_response_schema_benchmark_test.go
  hat/hatPeer/tr043c_response_schema_test.go
  scripts/benchmark-tr043c-response-schema.sh
  scripts/deliver-tr043c-response-schema.sh
  scripts/format-tr043c-response-schema.sh
  scripts/race-tr043c-response-schema.sh
  scripts/test-tr043c-response-schema-package.sh
  scripts/test-tr043c-response-schema.sh
  scripts/vet-tr043c-response-schema.sh
)

case "$mode" in
inspect)
  git status --short
  git diff --cached --stat
  git diff --cached --check
  ;;
stage)
  git add -- "${files[@]}"
  git diff --cached --check
  git diff --cached --stat
  ;;
commit)
  git commit -m "feat: negotiate compact peer response schemas"
  ;;
push)
  git push origin HEAD:master
  ;;
*)
  printf 'usage: %s [inspect|stage|commit|push]\n' "$0" >&2
  exit 2
  ;;
esac
