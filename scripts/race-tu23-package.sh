#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -count=1
