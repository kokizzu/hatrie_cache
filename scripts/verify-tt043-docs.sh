#!/bin/sh
set -eu

test -s TT043_MAINTENANCE_READ_ONLY.md
rg -n '^# TT-043 Maintenance Read-Only Mode$|maintenance_read_only|423 Locked|FailedPrecondition' TT043_MAINTENANCE_READ_ONLY.md
rg -n 'TT043_MAINTENANCE_READ_ONLY\.md' README.md
rg -n '^\| TT-043 \|.*Implemented' ENGINE_IDEAS.md
rg -n '^## TT-043 Maintenance Read-Only Admission$|Maintenance flag off|Maintenance flag on' BENCHMARK.md
