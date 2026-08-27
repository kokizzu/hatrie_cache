#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/regex.go ./hat/hatSql/regex_test.go
