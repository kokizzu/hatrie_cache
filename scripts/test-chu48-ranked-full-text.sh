#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestCHU48Ranked' -count=1
