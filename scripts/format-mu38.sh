#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
gofmt -w hat/hatStorage/immutable_part_manifest.go hat/hatStorage/mu38_immutable_part_manifest_test.go hat/hatStorage/mu38_immutable_part_manifest_benchmark_test.go
