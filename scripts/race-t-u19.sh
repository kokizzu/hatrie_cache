#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run 'TU19' -count=1
