#!/bin/sh
set -eu
test -s TR049_QUEUE_PARTITION_OWNERSHIP.md
rg -n '^# TR-49 Queue Partition Ownership And Online Migration$' TR049_QUEUE_PARTITION_OWNERSHIP.md
rg -n '^## TR-49: Queue partition ownership and online migration$' BENCHMARK.md
rg -n 'TR049_QUEUE_PARTITION_OWNERSHIP.md|tr-49-queue-partition-ownership-and-online-migration' README.md
rg -n '^\| TR-49 \|.*\[x\].*TR049_QUEUE_PARTITION_OWNERSHIP.md' INSPIRATION_BACKLOG.md
