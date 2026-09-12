#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSnapshot/snapshot.go hat/hatCache/snapshot_manifest.go hat/hatCache/t_u08_snapshot_manifest_test.go hat/hatCache/t_u08_snapshot_manifest_benchmark_test.go
