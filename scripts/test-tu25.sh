#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTU25' -count=1
