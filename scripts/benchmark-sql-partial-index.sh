#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONPartialIndexRefresh$' -benchmem -count=5
