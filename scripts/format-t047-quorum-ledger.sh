#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatReplication/t047_quorum_ledger.go \
  hat/hatReplication/t047_quorum_ledger_test.go \
  hat/hatReplication/t047_quorum_ledger_benchmark_test.go
