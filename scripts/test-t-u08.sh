#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCommandJournalWriteSnapshotWithManifest$' -count=1
