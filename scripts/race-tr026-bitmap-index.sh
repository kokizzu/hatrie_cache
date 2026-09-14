#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestTR026' -count=1
