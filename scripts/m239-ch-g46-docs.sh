#!/usr/bin/env bash
set -euo pipefail

test -s CH046_DICTIONARY_NEGATIVE_CACHE.md
grep -Fq 'CH046_DICTIONARY_NEGATIVE_CACHE.md' README.md
grep -Fq 'CH046 Dictionary Negative Cache' BENCHMARK.md
grep -Fq 'CH-G46' IDEA_GAP_CATALOG.md
