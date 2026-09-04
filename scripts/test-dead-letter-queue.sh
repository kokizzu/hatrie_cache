#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^TestDeadLetterQueue' -count=1
