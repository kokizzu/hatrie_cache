#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestTypedTable(AdaptiveDictionary|DictionaryAdaptive|DictionaryEncodedAllNull)' -count=1
