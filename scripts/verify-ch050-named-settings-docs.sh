#!/usr/bin/env bash
set -euo pipefail

rg -n 'SQLNamedSettingsRegistry|SQL_NAMED_SETTINGS.md|CH-050|Named settings collections' SQL_NAMED_SETTINGS.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md
