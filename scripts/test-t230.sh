#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT230' -count=1
