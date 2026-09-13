#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkStorageKeyPinningCandidateAdmission$' -benchmem -count=5
