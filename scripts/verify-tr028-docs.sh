#!/usr/bin/env bash
set -euo pipefail

for file in \
  TR028_AUTHENTICATED_KEYSET_TOKENS.md \
  README.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md
do
  test -s "$file"
done

rg -q 'TR028_AUTHENTICATED_KEYSET_TOKENS.md' README.md INSPIRATION_BACKLOG.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -q 'KeysetCursorTokenCodec' README.md TR028_AUTHENTICATED_KEYSET_TOKENS.md BENCHMARK.md
rg -q 'TR-028 Authenticated SQL Keyset Cursor Tokens' BENCHMARK.md
