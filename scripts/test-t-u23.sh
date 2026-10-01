#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTU23' -count=1
