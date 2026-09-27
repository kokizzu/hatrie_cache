#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/c154_replication_schema_rollout.go \
  hat/hatCache/c154_replication_schema_rollout_test.go
