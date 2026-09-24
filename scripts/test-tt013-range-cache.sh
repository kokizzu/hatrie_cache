#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTT013' -count=1
