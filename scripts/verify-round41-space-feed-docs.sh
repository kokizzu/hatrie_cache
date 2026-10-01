#!/usr/bin/env bash
set -euo pipefail

test -s TU39_SPACE_CHANGEFEED.md
rg -q 'TU39_SPACE_CHANGEFEED.md' README.md
rg -q 'T-U39 .*Implemented' PRODUCT_IDEA_GAPS.md
printf '%s\n' 'T-U39 documentation links and catalog status verified.'
