#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/backup_context.go \
  hat/hatCache/backup_bundle.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/backup_context_test.go
