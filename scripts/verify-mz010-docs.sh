#!/usr/bin/env bash
set -euo pipefail

test -f MZ010_JOURNAL_SUBSCRIPTIONS.md
rg -F 'MZ010_JOURNAL_SUBSCRIPTIONS.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'MZ-010' ENGINE_IDEAS.md BENCHMARK.md MZ010_JOURNAL_SUBSCRIPTIONS.md
rg -F 'BENCHMARK.md#mz-010-command-journal-subscriptions' MZ010_JOURNAL_SUBSCRIPTIONS.md ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'MZ-010 documentation links and references verified.'
