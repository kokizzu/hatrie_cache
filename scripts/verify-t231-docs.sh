#!/usr/bin/env bash
set -euo pipefail

rg -n 'TT031|T031|AfterReplace|transaction identity' README.md BENCHMARK.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md TT031_AFTER_REPLACE_AUDIT.md
