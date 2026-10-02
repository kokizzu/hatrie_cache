#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestJournalWriteQuorumState' -count=1
