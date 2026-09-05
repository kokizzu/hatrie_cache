#!/bin/sh
set -eu

rg -n 'QueryTraceRecorder|OpenTelemetrySpans|query_trace' README.md QUERY_TRACING.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
