#!/bin/sh
set -eu

go test -race ./hat/hatSql ./hat/hatCache -run '^Test(SQLColumnarLayoutBuildsNGram|ColumnarStringNGram|ColumnarNGram)'
