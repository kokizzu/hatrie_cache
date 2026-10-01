#!/usr/bin/env bash
set -euo pipefail

test -s TU13_DURABLE_MEMBERSHIP.md
rg -q 'TU13_DURABLE_MEMBERSHIP.md' README.md
rg -q 'T-U13 Durable Membership' BENCHMARK.md
rg -q 'T-U13.*Durable cluster membership' PRODUCT_IDEA_GAPS.md
rg -q 'UnsafeNoSync' TU13_DURABLE_MEMBERSHIP.md
rg -q 'fsync' TU13_DURABLE_MEMBERSHIP.md
printf '%s\n' 'round37 topology documentation verified'
