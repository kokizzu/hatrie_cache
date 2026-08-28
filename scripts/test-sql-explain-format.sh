#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestMarshalExplainJSONAndDOT$' -count=1
