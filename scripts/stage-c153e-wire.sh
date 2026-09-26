#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage C153e: the index is not empty' >&2
	exit 1
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153e-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

git show HEAD:Makefile > "$tmp_dir/Makefile"
cat >> "$tmp_dir/Makefile" <<'EOF'

.PHONY: benchmark-c153e-wire
benchmark-c153e-wire:
	sh scripts/benchmark-c153e-wire.sh

.PHONY: test-c153e-wire
test-c153e-wire:
	sh scripts/test-c153e-wire.sh

.PHONY: format-c153e-wire
format-c153e-wire:
	sh scripts/format-c153e-wire.sh

.PHONY: test-c153e-package
test-c153e-package:
	sh scripts/test-c153e-package.sh

.PHONY: race-c153e-package
race-c153e-package:
	sh scripts/race-c153e-package.sh

.PHONY: vet-c153e-package
vet-c153e-package:
	sh scripts/vet-c153e-package.sh

.PHONY: stage-c153e-wire
stage-c153e-wire:
	sh scripts/stage-c153e-wire.sh

.PHONY: commit-c153e-wire
commit-c153e-wire:
	sh scripts/commit-c153e-wire.sh

.PHONY: push-c153e-wire
push-c153e-wire:
	sh scripts/push-c153e-wire.sh
EOF

cat > "$tmp_dir/inspiration-extra" <<'EOF'
- [x] C153e Bounded deterministic binary vote transfer codec. `hatTopology`
  now exports validated `MarshalPartitionOwnershipConsensusVote` and
  `UnmarshalPartitionOwnershipConsensusVote` APIs that carry both legacy and
  authenticated votes with strict size, canonical-varint, and field validation;
  HTTP/gRPC session wiring remains caller-owned. See
  [C153E_PARTITION_OWNERSHIP_WIRE.md](C153E_PARTITION_OWNERSHIP_WIRE.md).
EOF
git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk -v extra_file="$tmp_dir/inspiration-extra" '
BEGIN {
	while ((getline line < extra_file) > 0) extra = extra line ORS
	close(extra_file)
}
!inserted && index($0, "C154 Rolling schema changes across replicas.") {
	printf "%s", extra
	inserted = 1
}
{ print }
END {
	if (!inserted) {
		print "could not find C154 insertion point" > "/dev/stderr"
		exit 1
	}
}
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.next"
mv "$tmp_dir/INSPIRATION.next" "$tmp_dir/INSPIRATION.md"

git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
cat >> "$tmp_dir/BENCHMARK.md" <<'EOF'

<a id="c153e-partition-ownership-wire"></a>
## C153e Partition-Ownership Vote Wire Codec

The bounded `hatTopology.MarshalPartitionOwnershipConsensusVote` and
`UnmarshalPartitionOwnershipConsensusVote` APIs carry legacy or authenticated
partition-ownership votes in a deterministic binary format for caller-owned
HTTP/gRPC transport. They validate field sizes, canonical varints, metadata,
flags, signature shape, and trailing bytes before returning a vote.

| Operation | JSON baseline | Binary codec | Improvement |
|---|---:|---:|---:|
| marshal | 570.3 ns/op, 432 B/op, 2 allocs | 126.6 ns/op, 160 B/op, 1 alloc | 4.50x faster, 2.70x less allocation bytes |
| unmarshal | 3,212 ns/op, 608 B/op, 17 allocs | 220.0 ns/op, 136 B/op, 8 allocs | 14.60x faster, 4.47x less allocation bytes |
| payload | 266 bytes | 118 bytes | 2.25x smaller, 55.6% less bandwidth |

Raw five-sample output from `make benchmark-c153e-wire` is recorded in
[C153E_PARTITION_OWNERSHIP_WIRE.md](C153E_PARTITION_OWNERSHIP_WIRE.md).

```text
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2121344  568.6 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2109584  572.5 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2150533  560.0 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2098915  570.3 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2084490  578.3 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  364878  3195 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  370466  3244 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  380785  3219 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  366949  3212 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  337366  3212 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9559248  126.0 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9248680  124.4 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9131565  126.8 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9407535  126.6 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9348489  127.1 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5222091  228.5 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5362305  220.0 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5400908  219.8 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5526069  220.0 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5241472  221.0 ns/op  136 B/op  8 allocs/op
```
EOF

blob=$(git hash-object -w "$tmp_dir/Makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
blob=$(git hash-object -w "$tmp_dir/INSPIRATION.md")
git update-index --add --cacheinfo "100644,$blob,INSPIRATION.md"
blob=$(git hash-object -w "$tmp_dir/BENCHMARK.md")
git update-index --add --cacheinfo "100644,$blob,BENCHMARK.md"

git add -- \
	C153E_PARTITION_OWNERSHIP_WIRE.md \
	hat/hatTopology/c153e_partition_ownership_wire.go \
	hat/hatTopology/c153e_partition_ownership_wire_test.go \
	scripts/benchmark-c153e-wire.sh \
	scripts/commit-c153e-wire.sh \
	scripts/format-c153e-wire.sh \
	scripts/push-c153e-wire.sh \
	scripts/race-c153e-package.sh \
	scripts/stage-c153e-wire.sh \
	scripts/test-c153e-package.sh \
	scripts/test-c153e-wire.sh \
	scripts/vet-c153e-package.sh

git diff --cached --check
git diff --cached --name-status
