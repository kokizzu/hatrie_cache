#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^Test(SQLColumnarLayoutBuildsNGram|ColumnarStringNGram|ColumnarNGram)'
