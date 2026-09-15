#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestFrontierReadHold' -count=1
