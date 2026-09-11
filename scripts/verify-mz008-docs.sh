#!/usr/bin/env bash
set -euo pipefail

rg -n --fixed-strings 'SQL_AS_OF.md' README.md
rg -n --fixed-strings 'AsOfFrontier' SQL_AS_OF.md hat/hatSql/query.go
rg -n --fixed-strings 'SQLFrontierSnapshotProvider' SQL_AS_OF.md hat/hatSql/contracts.go
rg -n --fixed-strings 'MZ-008 SQL AS OF historical reads' BENCHMARK.md
rg -n --fixed-strings 'MZ-008' ENGINE_IDEAS.md
rg -n --fixed-strings 'Historical `AS OF` reads' ADOPTED_QUERY_ENGINE_IDEAS.md
