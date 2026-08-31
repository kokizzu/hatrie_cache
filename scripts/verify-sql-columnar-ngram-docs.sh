#!/bin/sh
set -eu

rg -q "LIKE '%text%'" COLUMNAR_NGRAMS.md
rg -q 'benchmark-sql-columnar-ngram' COLUMNAR_NGRAMS.md
rg -q '\[Columnar n-gram sidecars\]\(COLUMNAR_NGRAMS.md\)' README.md
