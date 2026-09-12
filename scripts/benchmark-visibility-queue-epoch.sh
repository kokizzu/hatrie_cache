#!/bin/sh
set -eu

exec go test -run '^$' -bench '^BenchmarkVisibilityQueue(LeaseAck|LeaseAckResident256|LeaseWithTokenAckToken|LeaseWithTokenAckTokenResident256)$' -benchmem -count=3 ./hat/hatDataStructure
