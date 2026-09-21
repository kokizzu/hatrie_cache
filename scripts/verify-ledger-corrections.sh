#!/usr/bin/env bash
set -euo pipefail

grep -Fq '| CH-016 | Asynchronous insert queue | Partially adopted as the opt-in bounded `AsyncInsertBuffer`:' ENGINE_IDEAS.md
grep -Fq '| CH-037 | `ARRAY JOIN` | Adopted for direct row-source arrays with optional aliases, deterministic expansion, empty/NULL-array behavior, scalar rejection, row/work limits, and `EXPLAIN` output;' ENGINE_IDEAS.md

test ! -e scripts/inspect-next-open-candidates.sh
test ! -e scripts/inspect-ch016-insert-paths.sh
test ! -e scripts/inspect-ch037-array-join.sh

printf '%s\n' 'ledger corrections verified'
