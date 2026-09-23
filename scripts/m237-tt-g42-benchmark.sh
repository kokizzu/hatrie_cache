#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTTG42(SingleRelayBaseline|PeerRelayBackpressureObserve)$' -benchmem -count=5
