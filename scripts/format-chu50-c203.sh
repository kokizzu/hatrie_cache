#!/bin/sh
set -eu

gofmt -w \
	hat/hatBackup/model.go \
	hat/hatBackup/consistency.go \
	hat/hatBackup/chain.go \
	hat/hatCache/backup_bundle.go \
	hat/hatCache/backup_repository.go \
	hat/hatCache/backup_doctor.go \
	hat/hatCache/backup_restore.go \
	hat/hatCache/ch_u50_backup_consistency_test.go \
	hat/hatCache/ch_u50_backup_consistency_benchmark_test.go \
	hat/hatCache/backup_partition_restore_test.go
