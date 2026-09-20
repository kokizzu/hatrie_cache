#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatFiber -run '^TestTU30' -count=1
