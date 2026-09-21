#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run 'TestCompactionScheduler(SizeTieredSelection|TimeAwareSelection|SelectorCoalesces)' -count=1
