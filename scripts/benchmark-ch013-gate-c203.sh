#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH013MutationAdmissionOverhead$' -benchmem -count=5
