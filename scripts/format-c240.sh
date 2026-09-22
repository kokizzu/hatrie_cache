#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/read_only_backup.go \
	hat/hatCache/c240_read_only_backup_test.go
