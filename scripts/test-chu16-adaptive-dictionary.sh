#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTable(AdaptiveDictionary|ExplicitDictionary|DictionaryAdaptive|DictionaryEncoded)' -count=1
