#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexLiteralIN|^TestSQLJSONIndexUsesIndexForLiteralIN$' -count=1
