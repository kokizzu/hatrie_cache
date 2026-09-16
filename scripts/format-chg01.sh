#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/chg01_external_group_spill_test.go
