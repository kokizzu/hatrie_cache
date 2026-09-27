#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  MZ004_DURABLE_COMPACTION_JOBS.md
  hat/hatPipeline/mz004_durable_compaction_jobs.go
  hat/hatPipeline/mz004_durable_compaction_jobs_test.go
  hat/hatPipeline/mz004_durable_compaction_jobs_benchmark_test.go
  scripts/benchmark-mz004-durable-jobs.sh
  scripts/format-mz004-durable-jobs.sh
  scripts/race-mz004-durable-jobs.sh
  scripts/test-mz004-durable-jobs.sh
  scripts/test-mz004-package.sh
  scripts/vet-mz004-durable-jobs.sh
  scripts/deliver-mz004-durable-jobs.sh
)

makefile_targets='.PHONY: benchmark-mz004-durable-jobs
benchmark-mz004-durable-jobs:
	bash ./scripts/benchmark-mz004-durable-jobs.sh

.PHONY: test-mz004-durable-jobs
test-mz004-durable-jobs:
	bash ./scripts/test-mz004-durable-jobs.sh

.PHONY: format-mz004-durable-jobs
format-mz004-durable-jobs:
	bash ./scripts/format-mz004-durable-jobs.sh

.PHONY: race-mz004-durable-jobs
race-mz004-durable-jobs:
	bash ./scripts/race-mz004-durable-jobs.sh

.PHONY: vet-mz004-durable-jobs
vet-mz004-durable-jobs:
	bash ./scripts/vet-mz004-durable-jobs.sh

.PHONY: test-mz004-package
test-mz004-package:
	bash ./scripts/test-mz004-package.sh

.PHONY: stage-mz004-durable-jobs
stage-mz004-durable-jobs:
	bash ./scripts/deliver-mz004-durable-jobs.sh stage

.PHONY: deliver-mz004-durable-jobs
deliver-mz004-durable-jobs:
	bash ./scripts/deliver-mz004-durable-jobs.sh deliver'

benchmark_section='## MZ-004 Durable Compaction Jobs

Command: `make benchmark-mz004-durable-jobs` on Linux/amd64, AMD Ryzen 9
5950X, five samples per benchmark, `-benchmem`. The fixture contains 128
bounded jobs with the same IDs, frontier IDs, boundaries, and states for both
codecs. The JSON rows are the pre-implementation baseline; the binary rows are
the new `HCJ1` snapshot codec.

| Operation | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| JSON encode baseline | 15,575 | 8,254 | 2 | 1.00x |
| `HCJ1` binary encode | 2,613 | 4,864 | 1 | 5.96x faster; 1.70x lower heap |
| JSON decode baseline | 106,570 | 14,328 | 144 | 1.00x |
| `HCJ1` binary decode | 4,965 | 7,424 | 129 | 21.46x faster; 1.93x lower heap |
| Snapshot wire size | 7,992 bytes | 1,806 bytes | n/a | 4.43x smaller |

Raw samples:

```text
JSON encode: 15575 15463 15873 15697 15563 ns/op; 8254-8256 B/op; 2 allocs/op
JSON decode: 106570 105688 106255 107503 107938 ns/op; 14328 B/op; 144 allocs/op
HCJ1 encode: 2600 2613 2670 2774 2594 ns/op; 4864 B/op; 1 alloc/op; 1806 wire bytes
HCJ1 decode: 4989 4898 4807 5074 4965 ns/op; 7424 B/op; 129 allocs/op; 1806 wire bytes
```

The ledger is opt-in and does not add work to the ordinary scheduler or
frontier paths. It trades one bounded in-memory job record per retained task
and an explicit durable save for restart recovery; automatic task recovery,
priority selection, and coalescing remain caller policy. See
[MZ004_DURABLE_COMPACTION_JOBS.md](MZ004_DURABLE_COMPACTION_JOBS.md).
'

engine_line='| MZ-004 | Logical compaction controls | Partially adopted: opt-in `hatPipeline.FrontierCompactionScheduler` and `FrontierRetentionRegistry.WaitUntilSafe` gate caller-owned history removal behind source frontiers and read holds; `SetPolicy`/`ClearPolicy` add bounded per-frontier outstanding-task admission, and `FrontierCompactionJobLedger` adds bounded CRC-checked durable job snapshots with running-job recovery. Automatic priority/coalescing and SQL wiring remain open. See [MZ004_COMPACTION_POLICY.md](MZ004_COMPACTION_POLICY.md) and [MZ004_DURABLE_COMPACTION_JOBS.md](MZ004_DURABLE_COMPACTION_JOBS.md). | High |'

adopted_line='| Materialize | Frontier-aware logical compaction | Partially adopted as an opt-in maintenance admission layer | `hatPipeline.FrontierCompactionScheduler` and `FrontierRetentionRegistry.WaitUntilSafe` wait for a safe lower frontier and released historical-read leases before queueing caller-owned compaction work; `FrontierCompactionJobLedger` adds bounded CRC-checked durable job snapshots with running-job recovery. Priorities, coalescing, and automatic SQL integration remain caller-owned/open. See [MZ003_FRONTIER_COMPACTION_SCHEDULER.md](MZ003_FRONTIER_COMPACTION_SCHEDULER.md), [MZ004_DURABLE_COMPACTION_JOBS.md](MZ004_DURABLE_COMPACTION_JOBS.md), and [BENCHMARK.md#mz-004-durable-compaction-jobs](BENCHMARK.md#mz-004-durable-compaction-jobs). |'

usage() {
  printf '%s\n' "usage: $0 status|stage|deliver"
}

stage_shared_files() (
  local temp_dir makefile_temp benchmark_temp engine_temp adopted_temp hash
  temp_dir=$(mktemp -d)
  trap 'rm -rf "$temp_dir"' EXIT

  if ! git diff --cached --quiet -- Makefile BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md; then
    printf '%s\n' 'refusing to replace already-staged shared-file changes' >&2
    return 1
  fi

  makefile_temp="$temp_dir/Makefile"
  {
    git show HEAD:Makefile
    printf '\n%s\n' "$makefile_targets"
  } > "$makefile_temp"
  hash=$(git hash-object -w "$makefile_temp")
  git update-index --add --cacheinfo "100644,$hash,Makefile"

  benchmark_temp="$temp_dir/BENCHMARK.md"
  git show HEAD:BENCHMARK.md | awk -v addition="$benchmark_section" '
    /^<a id="rejected-t042-counter-parallel-replay"><\/a>$/ {
      print addition
      print
      next
    }
    { print }
  ' > "$benchmark_temp"
  hash=$(git hash-object -w "$benchmark_temp")
  git update-index --add --cacheinfo "100644,$hash,BENCHMARK.md"

  engine_temp="$temp_dir/ENGINE_IDEAS.md"
  git show HEAD:ENGINE_IDEAS.md | awk -v replacement="$engine_line" '
    /^\| MZ-004 \| Logical compaction controls \|/ {
      print replacement
      next
    }
    { print }
  ' > "$engine_temp"
  hash=$(git hash-object -w "$engine_temp")
  git update-index --add --cacheinfo "100644,$hash,ENGINE_IDEAS.md"

  adopted_temp="$temp_dir/ADOPTED_QUERY_ENGINE_IDEAS.md"
  git show HEAD:ADOPTED_QUERY_ENGINE_IDEAS.md | awk -v replacement="$adopted_line" '
    /^\| Materialize \| Frontier-aware logical compaction \|/ {
      print replacement
      next
    }
    { print }
  ' > "$adopted_temp"
  hash=$(git hash-object -w "$adopted_temp")
  git update-index --add --cacheinfo "100644,$hash,ADOPTED_QUERY_ENGINE_IDEAS.md"
)

stage_feature() {
  git add -- "${feature_files[@]}"
  if git diff --cached --quiet -- Makefile BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md; then
    stage_shared_files
  fi
  git diff --cached --check
  git diff --cached --name-status
}

case "${1:-}" in
  status)
    git status --short
    ;;
  stage)
    stage_feature
    ;;
  deliver)
    stage_feature
    git commit -m 'feat: add durable compaction job ledger [skip ci]'
    git push origin HEAD
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
