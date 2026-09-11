#!/usr/bin/env bash
set -euo pipefail

test -f MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md
rg -F 'MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'MZ-013' ENGINE_IDEAS.md BENCHMARK.md MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md
rg -F 'BENCHMARK.md#mz-013-source-connector-checkpoints' MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'make test-mz013-source' MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md
rg -F 'make benchmark-mz013-source' MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md BENCHMARK.md
printf '%s\n' 'MZ-013 documentation links, benchmark references, and commands verified.'
