#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestC247' -count=1
