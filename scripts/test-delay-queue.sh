#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^TestDelayQueue' -count=1
