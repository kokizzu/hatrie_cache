#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/model.go ./hat/hatSql/query.go ./hat/hatSql/explain_lineage_test.go
