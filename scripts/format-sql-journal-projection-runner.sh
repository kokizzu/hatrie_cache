#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_incremental_projection.go hat/hatCache/sql_incremental_projection_test.go
