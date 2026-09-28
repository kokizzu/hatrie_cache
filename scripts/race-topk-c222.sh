#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure/top_k.go ./hat/hatDataStructure/top_k_test.go -run '^TestTopK'
