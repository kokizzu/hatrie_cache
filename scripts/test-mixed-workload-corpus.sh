#!/bin/sh
set -eu

go test ./cmd/hatrie-sqlbench -run '^TestRunReportsThroughputAllocationSpillAndPlan$' -count=1
