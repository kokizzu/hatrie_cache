#!/usr/bin/env bash
set -euo pipefail

cd /tmp/hatrie-tu09-stable-20261006
go test ./hat/hatCache -run 'TestJoinFromSnapshotAndJournal' -count=1
