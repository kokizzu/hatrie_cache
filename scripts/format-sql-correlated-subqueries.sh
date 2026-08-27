#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/subquery.go ./hat/hatSql/correlated_subquery_test.go
