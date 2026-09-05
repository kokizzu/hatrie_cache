#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestTypedTableAggregate' -count=1
