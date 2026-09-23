#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^TestT229' -count=1
