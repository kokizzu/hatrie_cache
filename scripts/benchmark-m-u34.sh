#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatCache \
	-run '^$' \
  -bench '^BenchmarkCommandJournalSubscriptionReplay100(WithCheckpoint|WithFileCheckpointBatchAck)?$' \
  -benchmem \
  -count=5
