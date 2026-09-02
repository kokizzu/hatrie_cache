#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/materialized.go hat/hatSql/projection_selection_test.go
