#!/usr/bin/env bash
set -euo pipefail

git diff --stat
git diff -- BENCHMARK.md IDEA_GAP_CATALOG.md Makefile hat/hatSql/contracts.go hat/hatSql/query.go CH006_RUNTIME_JOIN_PREFETCH.md scripts/m269-ch-g06-rejection-docs.sh
