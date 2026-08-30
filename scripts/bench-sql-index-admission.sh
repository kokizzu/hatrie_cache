#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONIndexAdmissionDenied$' -benchmem -count=5
