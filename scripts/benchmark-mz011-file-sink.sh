#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz011-file-sink-bench.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT
mkdir -p "$temporary_root/go-tmp"
export GOCACHE="$temporary_root/go-cache"
export GOTMPDIR="$temporary_root/go-tmp"
cd "$root_dir"
go test ./hat/hatCache -run '^$' -bench '^BenchmarkMZ012(ExactlyOnceSinkBatch100|BaselineAtLeastOnceSinkBatch100)$' -benchmem -benchtime=100x -count=5
go test ./hat/hatCache -run '^$' -bench '^BenchmarkMZ011FileSink(CommitBatch100|ReadBatch100)$' -benchmem -benchtime=20x -count=5
