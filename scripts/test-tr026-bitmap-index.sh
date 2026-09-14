#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTR026' -count=1
