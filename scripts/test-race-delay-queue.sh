#!/bin/sh
set -eu

go test -race ./hat/hatDataStructure -run '^TestDelayQueue' -count=1
