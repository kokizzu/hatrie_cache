#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'Benchmark(ObserveTargetWireBytes|ObserveTargetBatchItems|SnapshotWithWireBytes)$' -benchmem -benchtime=100ms -count=5
