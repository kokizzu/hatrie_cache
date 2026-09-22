#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'README beginning:'
sed -n '1,24p' README.md
printf '%s\n' 'BENCHMARK beginning:'
sed -n '1,28p' BENCHMARK.md
printf '%s\n' 'T029 references:'
rg -n 'TT029|T029|BeforeReplace' README.md BENCHMARK.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md TT029_BEFORE_REPLACE.md
