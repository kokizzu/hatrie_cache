#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestCH025' -count=1
