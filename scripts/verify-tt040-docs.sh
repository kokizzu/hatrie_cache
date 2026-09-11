#!/bin/sh
set -eu

test -s TT040_SPACE_CHANGEFEED.md
rg -q '^# TT-040 Space Changefeed$' TT040_SPACE_CHANGEFEED.md
rg -q 'TT040_SPACE_CHANGEFEED.md' README.md
rg -q 'SubscribeSpace' TT040_SPACE_CHANGEFEED.md
rg -q 'CommandJournalSpaceSubscriptionReplay50Of100' BENCHMARK.md
rg -q '^\| TT-040 \|.*SubscribeSpace' ENGINE_IDEAS.md
