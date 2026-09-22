#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'TestWriteQuorumPolicy|TestExecuteCacheCommandAppliesPerSpaceWriteQuorum|TestExecuteAtomicBatchAppliesHighestPerSpaceWriteQuorum|TestPartitionedBatchDoesNotBypassPerSpaceWriteQuorum' -count=1
