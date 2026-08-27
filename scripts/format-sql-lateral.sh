#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/subquery.go ./hat/hatSql/lateral_test.go
