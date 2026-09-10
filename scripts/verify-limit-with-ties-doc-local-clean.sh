#!/usr/bin/env bash
set -euo pipefail

test -f LIMIT_WITH_TIES.md
rg -F '`LIMIT n WITH TIES`' LIMIT_WITH_TIES.md
rg -F 'MaxSortBytes' LIMIT_WITH_TIES.md
rg -F 'make benchmark-limit-with-ties-local-clean' LIMIT_WITH_TIES.md
rg -F '0.93x' LIMIT_WITH_TIES.md
rg -F '0.89x' LIMIT_WITH_TIES.md
rg -F 'forced external spill' LIMIT_WITH_TIES.md
