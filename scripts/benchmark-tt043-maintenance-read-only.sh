#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT043PublicCommandAdmission$' -benchmem -count=5
