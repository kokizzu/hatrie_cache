#!/bin/sh
set -eu

go test -race ./hat/hatFunction -run '^TestStoredFunction' -count=1 "$@"
