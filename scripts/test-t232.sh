#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT232' -count=1
