#!/usr/bin/env bash
set -euo pipefail

awk '
/^\| (CH|MZ|TR)-[0-9]+/ {
    print NR ":" $0
    rows++
}

END {
    if (rows == 0) {
        print "No CH/MZ/TR rows found in ENGINE_IDEAS.md"
        exit 1
    }
    print "Authoritative CH/MZ/TR rows: " rows
}
' ENGINE_IDEAS.md
