#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCompactionScheduler' -count=1
