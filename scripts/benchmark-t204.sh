#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^Benchmark(T204(AutomaticFailoverCommitLifecycle|SupervisedFailoverApprovalRecoveryLifecycle)|TU12AutomaticFailoverEvaluate)$' -benchmem -benchtime=200ms -count=5
