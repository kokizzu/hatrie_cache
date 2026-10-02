#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/journal_write_quorum.go hat/hatReplication/journal_write_quorum_test.go hat/hatReplication/journal_write_quorum_benchmark_test.go
