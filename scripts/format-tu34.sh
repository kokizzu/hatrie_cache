#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	api.go \
	hat/hatJournal/journal.go \
	hat/hatCache/journal.go \
	hat/hatCache/tu34_space_sync_policy_test.go \
	hat/hatCache/tu34_space_sync_policy_common_benchmark_test.go \
	hat/hatCache/tu34_space_sync_policy_benchmark_test.go
