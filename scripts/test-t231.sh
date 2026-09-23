#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -tags t231 -run '^TestT231' -count=1
