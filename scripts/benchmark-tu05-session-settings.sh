#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu05-benchmark.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

(cd "$root" && GOCACHE="$scratch/gocache" go test -run '^$' -bench 'BenchmarkTU05' -benchtime=100x -count=3 -benchmem ./hat/hatCache)
