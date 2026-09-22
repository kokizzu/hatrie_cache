#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU09(BaselineSequenceAdmission|BootstrapAdvanceWAL)$' -benchmem -count=5
