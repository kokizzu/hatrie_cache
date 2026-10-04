#!/bin/sh
set -eu

go test ./hat/hatFunction -run '^TestStoredFunction' -count=1 "$@"
