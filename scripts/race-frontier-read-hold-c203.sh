#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run 'TestFrontierReadHold' -count=1
