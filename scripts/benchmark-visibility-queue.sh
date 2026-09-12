#!/bin/sh
set -eu

exec go test -run '^$' -bench 'Benchmark(DelayQueuePushPop|VisibilityQueueLeaseAck|VisibilityQueueLeaseAckResident256|VisibilityQueueRequeueExpired)' -benchmem -count=3 ./hat/hatDataStructure
