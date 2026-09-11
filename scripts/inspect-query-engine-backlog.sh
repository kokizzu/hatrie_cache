#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== Unchecked inspiration items ====='
rg -n '^\- \[ \] (C153 |C154 |M032 |M033 |M037 |M038 |M052 |M090 |T042 |T047 |T103 |T150)' INSPIRATION.md
printf '%s\n' '===== Adopted ClickHouse/Materialize/Tarantool items ====='
rg -n 'ClickHouse|Materialize|Tarantool' ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' '===== Remaining high-impact candidates ====='
rg -n -C 1 '^\- \[ \] (C153 |C154 |M032 |M033 |M037 |M038 |M052 |M090 |T042 |T047 |T103 |T150)' INSPIRATION.md
