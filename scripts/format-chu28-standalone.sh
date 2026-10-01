#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatStorage/compaction_scheduler.go hat/hatStorage/ch_u28_legacy_fastpath_test.go
