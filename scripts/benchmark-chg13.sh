#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^Benchmark(FailoverExistingElectionControl|PlanFailoverAutomatic|PlanFailoverDisabled)$' -benchmem -count=5
