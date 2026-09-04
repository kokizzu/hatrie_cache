#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/replication.go \
  hat/hatCache/replication_task_ownership_test.go
