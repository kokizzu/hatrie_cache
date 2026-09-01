#!/bin/sh
set -eu

go test -race ./hat/hatCache -run '^TestSQLJSONIndexLiteralIN|^TestSQLJSONIndexUsesIndexForLiteralIN$' -count=1
