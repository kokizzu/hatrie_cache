#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/c213_typed_hash_join_test.go \
  hat/hatSql/c212_hash_join.go \
  hat/hatSql/query.go
