#!/usr/bin/env bash
set -euo pipefail

rg -n 'SQLQueryProfiler|SQL_QUERY_PROFILER.md|CH-032|Query profiler samples' SQL_QUERY_PROFILER.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md
