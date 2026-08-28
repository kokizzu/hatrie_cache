#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/external_quality.go ./hat/hatSql/external_quality_test.go
