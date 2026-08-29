#!/usr/bin/env sh
set -eu

grep -R -n 'func sqlJSONColumnarBatch' hat/hatCache --include='*.go' || true
