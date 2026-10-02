#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestJournalWriteQuorumState' -count=1
