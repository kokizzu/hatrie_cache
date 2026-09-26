#!/usr/bin/env bash
set -euo pipefail

workdir=$(mktemp -d /tmp/hatrie-ch012-baseline.XXXXXX)
trap 'rm -rf -- "$workdir"' EXIT
git archive --format=tar HEAD | tar -x -C "$workdir"
cd "$workdir"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH012ProjectionAdvisorCostBased$' -benchmem -count=5
