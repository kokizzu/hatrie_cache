#!/usr/bin/env bash
set -euo pipefail

test -f SNAPSHOT_RESTORE_WORKERS.md
rg -n 'ConfigureSnapshotRestoreWorkers|DefaultSnapshotRestoreWorkers|MZ-017|mz017-restore-workers' \
  README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md SNAPSHOT_RESTORE_WORKERS.md
