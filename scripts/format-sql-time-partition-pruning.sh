#!/bin/sh
set -eu

gofmt -w ./hat/hatCache/main.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_time_partition_pruning_test.go
