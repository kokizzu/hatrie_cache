#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/sequence.go hat/hatSql/sequence_test.go
