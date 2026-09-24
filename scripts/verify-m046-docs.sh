#!/usr/bin/env bash
set -euo pipefail

test -s M046_JSON_SUBCOLUMN_TOPN.md
test -s CH031_TYPED_JSON_SUBCOLUMNS.md
test -s BENCHMARK.md
test -s PRODUCT_IDEA_GAPS.md
test -s ADOPTED_QUERY_ENGINE_IDEAS.md

rg -q 'M046: Typed JSON Subcolumn Top-N' M046_JSON_SUBCOLUMN_TOPN.md
rg -q 'm046-typed-json-subcolumn-topn' BENCHMARK.md
rg -q 'M046_JSON_SUBCOLUMN_TOPN.md' CH031_TYPED_JSON_SUBCOLUMNS.md
rg -q 'M046_JSON_SUBCOLUMN_TOPN.md' PRODUCT_IDEA_GAPS.md
rg -q 'M046 bounded Top-N' ADOPTED_QUERY_ENGINE_IDEAS.md

printf '%s\n' 'M046 documentation verified'
