#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run 'Test(PriorityVisibilityQueue|VisibilityQueue)'
