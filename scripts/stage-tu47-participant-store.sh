#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage T047g: the index is not empty' >&2
	exit 1
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu47-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

git show HEAD:Makefile > "$tmp_dir/Makefile"
cat >> "$tmp_dir/Makefile" <<'EOF'

.PHONY: format-tu47-participant-store
format-tu47-participant-store:
	sh scripts/format-tu47-participant-store.sh

.PHONY: test-tu47-participant-store
test-tu47-participant-store:
	sh scripts/test-tu47-participant-store.sh

.PHONY: benchmark-tu47-participant-store
benchmark-tu47-participant-store:
	sh scripts/benchmark-tu47-participant-store.sh

.PHONY: test-tu47-package
test-tu47-package:
	sh scripts/test-tu47-package.sh

.PHONY: race-tu47-package
race-tu47-package:
	sh scripts/race-tu47-package.sh

.PHONY: vet-tu47-package
vet-tu47-package:
	sh scripts/vet-tu47-package.sh

.PHONY: stage-tu47-participant-store
stage-tu47-participant-store:
	sh scripts/stage-tu47-participant-store.sh

.PHONY: commit-tu47-participant-store
commit-tu47-participant-store:
	sh scripts/commit-tu47-participant-store.sh

.PHONY: push-tu47-participant-store
push-tu47-participant-store:
	sh scripts/push-tu47-participant-store.sh
EOF

cat > "$tmp_dir/inspiration-extra" <<'EOF'
- [x] T047g Durable participant state files. `ClusterWriteCommitParticipantFileStore` wraps deterministic participant snapshots in a bounded CRC32C envelope with private permissions, atomic same-directory replacement, and verified restore; checkpoint timing and coordinator integration remain caller-owned. See [T047G_DURABLE_PARTICIPANT_STATE.md](T047G_DURABLE_PARTICIPANT_STATE.md).
EOF
git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk -v extra_file="$tmp_dir/inspiration-extra" '
BEGIN {
	while ((getline line < extra_file) > 0) extra = extra line ORS
	close(extra_file)
}
!inserted && index($0, "T048") {
	printf "%s", extra
	inserted = 1
}
{ print }
END {
	if (!inserted) {
		print "could not find T048 insertion point" > "/dev/stderr"
		exit 1
	}
}
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.next"
mv "$tmp_dir/INSPIRATION.next" "$tmp_dir/INSPIRATION.md"

git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
cat >> "$tmp_dir/BENCHMARK.md" <<'EOF'

<a id="t047g-durable-participant-state"></a>
## T047g Durable Participant State

The opt-in `ClusterWriteCommitParticipantFileStore` persists the existing deterministic HCP1 participant snapshot in a bounded CRC32C envelope. It uses a same-directory temporary file, `0600` file permissions, `fsync`, atomic rename, and verified restore. It is checkpoint I/O, not a change to the prepare/commit/abort hot path.

| Operation | Median ns/op | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| snapshot marshal | 155.8 | 184 | 3 |
| durable save | 1,438,291 | 1,607 | 20 |
| durable load | 7,290 | 1,552 | 8 |

Raw five-sample output from `make benchmark-tu47-participant-store`:

```text
BenchmarkTU047ParticipantFileStore/marshal-32          7841767  155.8 ns/op  184 B/op  3 allocs/op
BenchmarkTU047ParticipantFileStore/marshal-32          7590798  162.7 ns/op  184 B/op  3 allocs/op
BenchmarkTU047ParticipantFileStore/marshal-32          7509488  156.2 ns/op  184 B/op  3 allocs/op
BenchmarkTU047ParticipantFileStore/marshal-32          7889265  155.5 ns/op  184 B/op  3 allocs/op
BenchmarkTU047ParticipantFileStore/marshal-32          7749588  155.1 ns/op  184 B/op  3 allocs/op
BenchmarkTU047ParticipantFileStore/save-32             632  2838607 ns/op  1607 B/op  20 allocs/op
BenchmarkTU047ParticipantFileStore/save-32             842  1438291 ns/op  1607 B/op  20 allocs/op
BenchmarkTU047ParticipantFileStore/save-32             825  1487574 ns/op  1607 B/op  20 allocs/op
BenchmarkTU047ParticipantFileStore/save-32             865  1429323 ns/op  1607 B/op  20 allocs/op
BenchmarkTU047ParticipantFileStore/save-32             804  1424578 ns/op  1607 B/op  20 allocs/op
BenchmarkTU047ParticipantFileStore/load-32           159990     7284 ns/op  1552 B/op  8 allocs/op
BenchmarkTU047ParticipantFileStore/load-32           164782     7166 ns/op  1552 B/op  8 allocs/op
BenchmarkTU047ParticipantFileStore/load-32           160290     7290 ns/op  1552 B/op  8 allocs/op
BenchmarkTU047ParticipantFileStore/load-32           155600     7374 ns/op  1552 B/op  8 allocs/op
BenchmarkTU047ParticipantFileStore/load-32           160782     7306 ns/op  1552 B/op  8 allocs/op
```

The file store is intentionally much more expensive than in-memory participant operations because it includes local durable filesystem work. The tradeoff is bounded restart recovery without changing normal write-path latency. The CRC32C envelope detects accidental or partial corruption; it is not authentication or encryption.
EOF

blob=$(git hash-object -w "$tmp_dir/Makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
blob=$(git hash-object -w "$tmp_dir/INSPIRATION.md")
git update-index --add --cacheinfo "100644,$blob,INSPIRATION.md"
blob=$(git hash-object -w "$tmp_dir/BENCHMARK.md")
git update-index --add --cacheinfo "100644,$blob,BENCHMARK.md"

git add -- \
	hat/hatReplication/tu47_cluster_write_commit_participant_store.go \
	hat/hatReplication/tu47_cluster_write_commit_participant_store_test.go \
	T047G_DURABLE_PARTICIPANT_STATE.md \
	scripts/benchmark-tu47-participant-store.sh \
	scripts/format-tu47-participant-store.sh \
	scripts/race-tu47-package.sh \
	scripts/test-tu47-package.sh \
	scripts/test-tu47-participant-store.sh \
	scripts/vet-tu47-package.sh \
	scripts/stage-tu47-participant-store.sh \
	scripts/commit-tu47-participant-store.sh \
	scripts/push-tu47-participant-store.sh

git diff --cached --check
git diff --cached --name-status
