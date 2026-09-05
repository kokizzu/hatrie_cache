#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestExplainPipeline' -count=1
