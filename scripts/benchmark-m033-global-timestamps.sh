#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
	-run '^$' \
	-bench '^BenchmarkGlobalTimestampOracle(SnapshotJSON|SnapshotBinary|SnapshotBinaryDecode|SnapshotJSONDecode|FileStoreSave|FileStoreLoad)$' \
	-benchmem \
	-benchtime=300ms \
	-count=5
