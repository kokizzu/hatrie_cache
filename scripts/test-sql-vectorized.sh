#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLColumnarVectorGroupAggregate' -count=1
