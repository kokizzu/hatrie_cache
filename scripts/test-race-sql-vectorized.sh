#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLColumnarVectorGroupAggregate' -count=1
