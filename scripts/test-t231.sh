#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT231' -count=1
