#!/usr/bin/env bash
set -euo pipefail

bash -n scripts/benchmark-mz021-replica-hot-handoff.sh scripts/commit-mz021-replica-hot-handoff.sh scripts/format-mz021-replica-hot-handoff.sh scripts/push-mz021-replica-hot-handoff.sh scripts/race-mz021-replica-hot-handoff.sh scripts/test-mz021-package.sh scripts/test-mz021-replica-hot-handoff.sh scripts/verify-mz021-docs.sh scripts/vet-mz021-replica-hot-handoff.sh
test -s MZ021_REPLICA_HOT_HANDOFF.md
rg -F 'NewReplicaHotHandoff' MZ021_REPLICA_HOT_HANDOFF.md
rg -F 'MZ-021 | Replica hot handoff' ENGINE_IDEAS.md
rg -F 'MZ-021 Replica Hot Handoff' BENCHMARK.md
rg -F '2.27x faster' BENCHMARK.md
