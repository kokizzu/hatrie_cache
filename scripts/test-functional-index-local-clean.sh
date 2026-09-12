#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'FunctionalIndex' -count=1
