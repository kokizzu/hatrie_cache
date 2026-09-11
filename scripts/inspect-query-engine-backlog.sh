#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== Unchecked inspiration items ====='
rg -n '^\- \[ \]' INSPIRATION.md
printf '%s\n' '===== Adopted ClickHouse/Materialize/Tarantool items ====='
rg -n 'ClickHouse|Materialize|Tarantool' ADOPTED_QUERY_ENGINE_IDEAS.md
