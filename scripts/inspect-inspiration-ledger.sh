#!/usr/bin/env bash
set -euo pipefail

awk '
/^- \[[ x]\] [TC][0-9]+ / {
    total++
    if ($0 ~ /^- \[x\]/) {
        done++
    } else {
        print "NEXT " $0
        pending++
    }
}
END {
    printf "Summary: %d tracked ideas, %d complete, %d pending.\n", total, done, pending
}' INSPIRATION_ROUND2.md
