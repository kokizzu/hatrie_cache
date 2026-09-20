#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTTLRecompressionSeparatesRewriteFromDelete$' -count=1
