#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT229' -race -count=1
