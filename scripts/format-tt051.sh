#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/partitioned_command_journal.go hat/hatCache/tt051_partition_durability_test.go hat/hatCache/tt051_partition_durability_benchmark_test.go
