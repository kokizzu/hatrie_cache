#!/bin/sh
set -eu

go test ./hat/hatReplication -run '^TestConflictDecisionLog' -count=1 "$@"
