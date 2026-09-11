#!/usr/bin/env bash
set -euo pipefail

test -f PERSISTENT_QUERY_LOG.md
rg -F 'CH-031' ENGINE_IDEAS.md BENCHMARK.md
rg -F 'PERSISTENT_QUERY_LOG.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -F 'QueryLog' PERSISTENT_QUERY_LOG.md hat/hatSql/query_log.go
