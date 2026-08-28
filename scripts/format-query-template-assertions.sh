#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/template_assertion.go ./hat/hatSql/template_assertion_test.go
