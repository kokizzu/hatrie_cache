#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestLowCardinalityString' -count=1
