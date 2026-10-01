#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestMemtxRowTable' -count=1
