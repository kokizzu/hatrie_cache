#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

shared_files=(Makefile BENCHMARK.md INSPIRATION.md)
feature_files=(
  C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT.md
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

for path in "${shared_files[@]}"; do
  if ! git diff --cached --quiet -- "$path"; then
    printf 'refusing to overwrite staged changes in %s\n' "$path" >&2
    exit 1
  fi
done

git show HEAD:Makefile > "$tmp_dir/Makefile"
git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"

python3 - "$tmp_dir/Makefile" "$tmp_dir/BENCHMARK.md" "$tmp_dir/INSPIRATION.md" <<'PY'
from pathlib import Path
import sys

make_path, benchmark_path, inspiration_path = map(Path, sys.argv[1:])
make_block = """\

.PHONY: test-c154f-schema-barrier-snapshot benchmark-c154f-schema-barrier-baseline format-c154f-schema-barrier-snapshot benchmark-c154f-schema-barrier-snapshot test-c154f-schema-barrier-package race-c154f-schema-barrier-snapshot vet-c154f-schema-barrier-snapshot stage-c154f-schema-barrier-snapshot commit-c154f-schema-barrier-snapshot push-c154f-schema-barrier-snapshot
test-c154f-schema-barrier-snapshot:
\tbash scripts/test-c154f-schema-barrier-snapshot.sh

benchmark-c154f-schema-barrier-baseline:
\tbash scripts/benchmark-c154f-schema-barrier-baseline.sh

format-c154f-schema-barrier-snapshot:
\tbash scripts/format-c154f-schema-barrier-snapshot.sh

benchmark-c154f-schema-barrier-snapshot:
\tbash scripts/benchmark-c154f-schema-barrier-snapshot.sh

test-c154f-schema-barrier-package:
\tbash scripts/test-c154f-schema-barrier-package.sh

race-c154f-schema-barrier-snapshot:
\tbash scripts/race-c154f-schema-barrier-snapshot.sh

vet-c154f-schema-barrier-snapshot:
\tbash scripts/vet-c154f-schema-barrier-snapshot.sh

stage-c154f-schema-barrier-snapshot:
\tbash scripts/stage-c154f-schema-barrier-snapshot.sh

commit-c154f-schema-barrier-snapshot:
\tbash scripts/commit-c154f-schema-barrier-snapshot.sh

push-c154f-schema-barrier-snapshot:
\tbash scripts/push-c154f-schema-barrier-snapshot.sh
"""

make_text = make_path.read_text()
if "stage-c154f-schema-barrier-snapshot:" in make_text:
    raise SystemExit("C154f Makefile targets already exist in HEAD")
make_path.write_text(make_text.rstrip() + make_block)

benchmark_text = benchmark_path.read_text()
benchmark_anchor = '<a id="t047f-participant-reconciliation"></a>'
if benchmark_text.count(benchmark_anchor) != 1:
    raise SystemExit("expected exactly one T047f benchmark anchor")
benchmark_section = """<a id=\"c154f-schema-migration-barrier-snapshot\"></a>
### C154f schema migration barrier snapshot

Command: `make benchmark-c154f-schema-barrier-snapshot`.

The fixture contains 32 barriers with four dependencies and mixed prepared,
committed, and aborted states. Medians below are from five 200-ms samples on
the same AMD Ryzen 9 5950X host. JSON restore unmarshals and rebuilds the same
registry maps as binary restore.

| Operation | JSON baseline | Binary snapshot | Improvement |
| --- | ---: | ---: | ---: |
| Snapshot encode | 16,144 ns/op, 13,119 B/op, 58 allocs/op, 5,659 wire bytes | 7,852 ns/op, 2,560 B/op, 2 allocs/op, 1,852 wire bytes | 2.06x faster, 5.12x lower allocation bytes, 29x fewer allocs, 3.06x smaller wire payload |
| Full registry restore | 90,742 ns/op, 34,960 B/op, 554 allocs/op, 5,659 wire bytes | 18,070 ns/op, 24,232 B/op, 368 allocs/op, 1,852 wire bytes | 5.02x faster, 1.44x lower allocation bytes, 1.51x fewer allocs, 3.06x smaller wire payload |

Raw samples:

```text
JSON snapshot ns/op: 16042, 16187, 16036, 16363, 16144
Binary snapshot ns/op: 7616, 7633, 7852, 7973, 8064
JSON restore ns/op: 92299, 90577, 89853, 90742, 91006
Binary restore ns/op: 18070, 17803, 18481, 17886, 18246
```

Details: `C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT.md`.

"""
benchmark_path.write_text(benchmark_text.replace(benchmark_anchor, benchmark_section + benchmark_anchor, 1))

inspiration_text = inspiration_path.read_text()
inspiration_anchor = "- [x] C155 Rolling binary upgrades with compatibility gates for the gRPC and HTTP command protocols; schema compatibility remains tracked separately under C154."
if inspiration_text.count(inspiration_anchor) != 1:
    raise SystemExit("expected exactly one C155 inspiration anchor")
inspiration_section = """- [x] C154f Durable schema migration barrier snapshots with strict validation,
  CRC protection, and atomic restore. See
  [C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT.md](C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT.md).
"""
inspiration_path.write_text(inspiration_text.replace(inspiration_anchor, inspiration_section + inspiration_anchor, 1))
PY

for path in "${shared_files[@]}"; do
  blob=$(git hash-object -w "$tmp_dir/$path")
  git update-index --add --cacheinfo "100644,$blob,$path"
done

git add -- "${feature_files[@]}"

python3 - "${shared_files[*]}" "${feature_files[*]}" <<'PY'
from pathlib import Path
import subprocess
import sys

expected = set(sys.argv[1].split()) | set(sys.argv[2].split())
actual = set(subprocess.check_output(["git", "diff", "--cached", "--name-only"], text=True).splitlines())
if actual != expected:
    print("unexpected staged paths", file=sys.stderr)
    print("expected:", *sorted(expected), sep="\n", file=sys.stderr)
    print("actual:", *sorted(actual), sep="\n", file=sys.stderr)
    raise SystemExit(1)
print("staged paths:")
print(*sorted(actual), sep="\n")
PY

git diff --cached --check
