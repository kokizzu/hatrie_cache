#!/bin/sh
set -eu

go test -race ./hat/hatReplication -run '^TestConflictDecisionLog' -count=1 "$@"
