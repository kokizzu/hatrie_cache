#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT231' -race -count=1
