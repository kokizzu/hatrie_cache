#!/usr/bin/env bash
set -euo pipefail

rg -n \
  'TR003 Replica-Promotion Catch-Up Barrier|ReplicaPromotionBarrier|make benchmark-tr003-replica-promotion-barrier' \
  TR003_REPLICA_PROMOTION_BARRIER.md
rg -n 'TR-03.*\[x\].*TR003_REPLICA_PROMOTION_BARRIER.md' INSPIRATION_BACKLOG.md
rg -n 'TR003_REPLICA_PROMOTION_BARRIER.md|tr-003-replica-promotion-catch-up-barrier' README.md
rg -n '<a id="tr-003-replica-promotion-catch-up-barrier"></a>|## TR003 Replica-Promotion Catch-Up Barrier' BENCHMARK.md
