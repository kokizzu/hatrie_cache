#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/rewrite.go ./hat/hatSql/rewrite_test.go
