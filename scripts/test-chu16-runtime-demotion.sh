#!/bin/sh
set -eu

go test ./hat/hatSql \
  -run '^TestTypedTable(AdaptiveDictionary|ExplicitDictionary)' \
  -count=1
