#!/usr/bin/env bash
set -euo pipefail

printf 'Topology/replication/data-structure files:\n'
rg --files hat/hatTopology hat/hatReplication hat/hatDataStructure 2>/dev/null |
  rg -i 'partition|bucket|migration|topology|replica|shard'
printf '\nPartition/bucket/migration declarations:\n'
rg -n '^type |^func ' hat/hatTopology/topology.go hat/hatTopology/ownership.go hat/hatTopology/ownership_consensus.go
printf '\nTopology model:\n'
sed -n '1,230p' hat/hatTopology/topology.go
printf '\nOwnership model:\n'
sed -n '1,190p' hat/hatTopology/ownership.go
printf '\nOwnership consensus model:\n'
sed -n '1,230p' hat/hatTopology/ownership_consensus.go
printf '\nParallel root overlap:\n'
git -C /home/kyz/go/src/hatrie_cache status --short -- hat/hatTopology hat/hatReplication hat/hatDataStructure
