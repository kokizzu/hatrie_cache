#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-chu03-aggregate-registry.sh
go test ./hat/hatDataStructure -run 'Test(AggregateStateRegistry|AggregateStateEnvelope)' -count=1
bash scripts/race-chu03-aggregate-registry.sh
bash scripts/vet-chu03-aggregate-registry.sh
