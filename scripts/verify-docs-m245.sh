#!/usr/bin/env bash
set -euo pipefail

test -s M245_TIMESTAMP_TELEMETRY.md
rg -q 'M245 Timestamp Throughput And Input-to-Output Latency' M245_TIMESTAMP_TELEMETRY.md
rg -q 'M245 Timestamp Throughput And Input-to-Output Latency' BENCHMARK.md
rg -q 'M245 Timestamp throughput and input-to-output latency metrics' INSPIRATION_ROUND2.md
