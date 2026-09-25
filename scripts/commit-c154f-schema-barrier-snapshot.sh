#!/usr/bin/env bash
set -euo pipefail

expected=(
  BENCHMARK.md
  C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT.md
  INSPIRATION.md
  Makefile
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot.go
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot_test.go
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot_baseline_benchmark_test.go
  scripts/benchmark-c154f-schema-barrier-baseline.sh
  scripts/benchmark-c154f-schema-barrier-snapshot.sh
  scripts/commit-c154f-schema-barrier-snapshot.sh
  scripts/format-c154f-schema-barrier-snapshot.sh
  scripts/push-c154f-schema-barrier-snapshot.sh
  scripts/race-c154f-schema-barrier-snapshot.sh
  scripts/stage-c154f-schema-barrier-snapshot.sh
  scripts/test-c154f-schema-barrier-package.sh
  scripts/test-c154f-schema-barrier-snapshot.sh
  scripts/vet-c154f-schema-barrier-snapshot.sh
)

git diff --cached --check
python3 - "${expected[*]}" <<'PY'
import subprocess
import sys

expected = set(sys.argv[1].split())
actual = set(subprocess.check_output(["git", "diff", "--cached", "--name-only"], text=True).splitlines())
if actual != expected:
    print("staged scope does not match C154f feature", file=sys.stderr)
    print("expected:", *sorted(expected), sep="\n", file=sys.stderr)
    print("actual:", *sorted(actual), sep="\n", file=sys.stderr)
    raise SystemExit(1)
PY

git commit -m "feat: add durable schema barrier snapshots"
