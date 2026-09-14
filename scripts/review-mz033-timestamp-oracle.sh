#!/usr/bin/env bash
set -euo pipefail

test -f hat/hatReplication/global_timestamp_oracle.go
test -f hat/hatReplication/global_timestamp_oracle_test.go
test -f MZ033_TIMESTAMP_ORACLE.md
rg -n 'GlobalTimestampOracle|GlobalTimestampLease|MZ-033' hat/hatReplication MZ033_TIMESTAMP_ORACLE.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
git diff --check
