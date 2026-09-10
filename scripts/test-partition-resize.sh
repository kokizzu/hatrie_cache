#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPartition
test -f PARTITION_RESIZE.md
grep -Fq '[PARTITION_RESIZE.md](PARTITION_RESIZE.md)' README.md
grep -Fq 'hatPartition.PlanSplit' PARTITION_RESIZE.md
