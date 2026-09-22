#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT250' -count=1
