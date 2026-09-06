#!/bin/sh
set -eu

gofmt -w hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/explain_optimizer_test.go
