#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/backup_checksum.go \
	hat/hatCache/backup_rehearsal_checksum_test.go \
	hat/hatCache/backup_restore.go \
	hat/hatCache/backup_doctor.go \
	hat/hatBackup/reports.go
