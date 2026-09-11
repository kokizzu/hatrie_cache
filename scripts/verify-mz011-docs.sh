#!/usr/bin/env bash
set -euo pipefail

test -f MZ011_SINK_CONNECTORS.md
rg -F 'MZ011_SINK_CONNECTORS.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'MZ-011' ENGINE_IDEAS.md BENCHMARK.md MZ011_SINK_CONNECTORS.md
rg -F 'BENCHMARK.md#mz-011-sink-connectors' MZ011_SINK_CONNECTORS.md ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'MZ-011 documentation links and references verified.'
