#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestT231' -count=1
