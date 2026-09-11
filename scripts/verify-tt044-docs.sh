#!/usr/bin/env bash
set -euo pipefail

rg -n -F -- 'SCHEMA_MIGRATION_DRY_RUN.md' README.md
rg -n -F -- 'TT-044' ENGINE_IDEAS.md
rg -n -F -- 'Schema migration dry run' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F -- '## Schema Migration Dry Run' BENCHMARK.md
rg -n -F -- '375.6 ns' SCHEMA_MIGRATION_DRY_RUN.md
rg -n -F -- '375.6 ns' BENCHMARK.md
