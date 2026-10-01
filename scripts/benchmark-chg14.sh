#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^Benchmark(SaveTopologyExistingControl|DurableMembershipApply|DurableMembershipSnapshot)$' -benchmem -benchtime=100ms -count=5
