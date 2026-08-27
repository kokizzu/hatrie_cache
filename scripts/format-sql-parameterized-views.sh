#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/parameterized_view.go ./hat/hatSql/parameterized_view_test.go
