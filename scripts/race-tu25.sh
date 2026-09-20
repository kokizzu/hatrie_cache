#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestTU25' -count=1
