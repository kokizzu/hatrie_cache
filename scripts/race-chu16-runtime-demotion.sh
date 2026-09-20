#!/bin/sh
set -eu

go test -race ./hat/hatSql \
  -run '^TestTypedTable(AdaptiveDictionary|ExplicitDictionary)' \
  -count=1
