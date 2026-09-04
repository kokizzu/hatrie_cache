#!/bin/sh
set -eu

go test -race ./hat/hatDataStructure -run '^TestDeadLetterQueue' -count=1
