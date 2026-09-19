#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'Test(AggregateStateRegistry|AggregateStateEnvelope)' -count=1
