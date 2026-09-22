#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkT236(Native|Pipeline)ChannelMPSC$' -benchmem -count=5
