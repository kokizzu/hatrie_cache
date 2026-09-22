#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestT249' -count=1
