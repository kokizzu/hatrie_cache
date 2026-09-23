#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^TestT228' -race -count=1
