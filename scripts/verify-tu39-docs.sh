#!/usr/bin/env bash
set -euo pipefail

for path in TU39_SPACE_CHANGEFEED.md hat/hatReplication/space_changefeed.go hat/hatReplication/space_changefeed_test.go hat/hatReplication/space_changefeed_example_test.go; do
  [[ -f "$path" ]] || { printf 'missing required path: %s\n' "$path" >&2; exit 1; }
done
awk '/TU39_SPACE_CHANGEFEED.md/ { found=1 } END { if (!found) exit 1 }' README.md
awk '/TU39_SPACE_CHANGEFEED.md/ { found=1 } END { if (!found) exit 1 }' PRODUCT_IDEA_GAPS.md
awk '/T-U39 bounded named-space changefeed/ { found=1 } END { if (!found) exit 1 }' BENCHMARK.md
go test ./hat/hatReplication -run '^ExampleSpaceChangefeed$' -count=1
