#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/explain_format.go ./hat/hatSql/explain_format_test.go
