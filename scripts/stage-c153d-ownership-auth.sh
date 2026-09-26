#!/bin/sh
set -eu

if git diff --cached --quiet; then
    :
else
    printf '%s\n' 'Refusing to stage C153d while the index already contains changes' >&2
    exit 1
fi

stage_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153d-stage.XXXXXX")
trap 'rm -rf "$stage_dir"' EXIT

git show HEAD:INSPIRATION.md >"$stage_dir/INSPIRATION.md.base"
awk '
$0 == "- [ ] C154 Rolling schema changes across replicas." && !inserted {
    print "- [x] C153d Opt-in HMAC-SHA256 authentication for partition-ownership consensus"
    print "  votes. Invalid or unsigned votes are rejected before quorum evaluation while"
    print "  the legacy unsigned evaluator and routing hot path remain unchanged. See"
    print "  [C153D_PARTITION_OWNERSHIP_AUTH.md](C153D_PARTITION_OWNERSHIP_AUTH.md)."
    inserted = 1
}
{ print }
END {
    if (!inserted) {
        exit 1
    }
}
' "$stage_dir/INSPIRATION.md.base" >"$stage_dir/INSPIRATION.md"

git show HEAD:BENCHMARK.md >"$stage_dir/BENCHMARK.md.base"
awk '
$0 == "<a id=\"c154f-schema-migration-barrier-snapshot\"></a>" && !inserted {
    print "<a id=\"c153d-partition-ownership-authentication\"></a>"
    print "## C153d Partition-Ownership Vote Authentication"
    print ""
    print "Command: `make benchmark-c153d-ownership-auth`."
    print ""
    print "This is an opt-in control-plane security feature. The benchmark compares the"
    print "existing unsigned quorum evaluator with HMAC-SHA256 signing, verification, and"
    print "quorum evaluation for the same ownership vote. The routing hot path is not"
    print "changed."
    print ""
    print "Median of five samples:"
    print ""
    print "| Path | ns/op | B/op | allocs/op | Relative CPU | Relative bytes |"
    print "| --- | ---: | ---: | ---: | ---: | ---: |"
    print "| Legacy unsigned | 761.8 | 848 | 9 | 1.00x | 1.00x |"
    print "| Authenticated | 2,830 | 2,912 | 30 | 3.71x | 3.43x |"
    print ""
    print "The authenticated path is slower by design because it computes and verifies an"
    print "HMAC over the complete ownership metadata. The overhead is limited to callers"
    print "that opt into authenticated votes; the legacy evaluator remains available for"
    print "trusted in-process callers."
    print ""
    print "Raw output:"
    print ""
    print "```text"
    print "BenchmarkC153dPartitionOwnershipConsensus/legacy-32          1489860  809.5 ns/op  848 B/op  9 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/legacy-32          1532114  761.8 ns/op  848 B/op  9 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/legacy-32          1564852  782.1 ns/op  848 B/op  9 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/legacy-32          1374253  742.7 ns/op  848 B/op  9 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/legacy-32          1631161  736.3 ns/op  848 B/op  9 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/authenticated-32   377784  2881 ns/op  2912 B/op  30 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/authenticated-32   418466  2829 ns/op  2912 B/op  30 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/authenticated-32   410958  2816 ns/op  2912 B/op  30 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/authenticated-32   407202  2834 ns/op  2912 B/op  30 allocs/op"
    print "BenchmarkC153dPartitionOwnershipConsensus/authenticated-32   410437  2830 ns/op  2912 B/op  30 allocs/op"
    print "```"
    print ""
    print "See [C153D_PARTITION_OWNERSHIP_AUTH.md](C153D_PARTITION_OWNERSHIP_AUTH.md) for"
    print "the API and security boundary."
    print ""
    inserted = 1
}
{ print }
END {
    if (!inserted) {
        exit 1
    }
}
' "$stage_dir/BENCHMARK.md.base" >"$stage_dir/BENCHMARK.md"

git show HEAD:Makefile >"$stage_dir/Makefile.base"
awk '
{ print }
END {
    print ""
    print ".PHONY: test-c153d-ownership-auth format-c153d-ownership-auth benchmark-c153d-ownership-auth test-c153d-package race-c153d-package vet-c153d-package stage-c153d-ownership-auth commit-c153d-ownership-auth push-c153d-ownership-auth"
    print "test-c153d-ownership-auth:"
    print "\tsh scripts/test-c153d-ownership-auth.sh"
    print ""
    print "format-c153d-ownership-auth:"
    print "\tsh scripts/format-c153d-ownership-auth.sh"
    print ""
    print "benchmark-c153d-ownership-auth:"
    print "\tsh scripts/benchmark-c153d-ownership-auth.sh"
    print ""
    print "test-c153d-package:"
    print "\tsh scripts/test-c153d-package.sh"
    print ""
    print "race-c153d-package:"
    print "\tsh scripts/race-c153d-package.sh"
    print ""
    print "vet-c153d-package:"
    print "\tsh scripts/vet-c153d-package.sh"
    print ""
    print "stage-c153d-ownership-auth:"
    print "\tsh scripts/stage-c153d-ownership-auth.sh"
    print ""
    print "commit-c153d-ownership-auth:"
    print "\tsh scripts/commit-c153d-ownership-auth.sh"
    print ""
    print "push-c153d-ownership-auth:"
    print "\tsh scripts/push-c153d-ownership-auth.sh"
}
' "$stage_dir/Makefile.base" >"$stage_dir/Makefile"

git show HEAD:hat/hatTopology/ownership_consensus.go >"$stage_dir/ownership_consensus.go.base"
awk '
{
    print
    if (!inserted && $0 ~ /^[[:space:]]*Accepted[[:space:]]+bool[[:space:]]+`json:/) {
        print "\tKeyID     string             `json:\"key_id,omitempty\"`"
        print "\tSignature []byte             `json:\"signature,omitempty\"`"
        inserted = 1
    }
}
END {
    if (!inserted) {
        exit 1
    }
}
' "$stage_dir/ownership_consensus.go.base" >"$stage_dir/ownership_consensus.go"

for path in INSPIRATION.md BENCHMARK.md Makefile hat/hatTopology/ownership_consensus.go; do
    case "$path" in
        INSPIRATION.md|BENCHMARK.md|Makefile)
            candidate="$stage_dir/$path"
            ;;
        *)
            candidate="$stage_dir/ownership_consensus.go"
            ;;
    esac
    hash=$(git hash-object -w "$candidate")
    git update-index --add --cacheinfo "100644,$hash,$path"
done

git add -- \
    C153D_PARTITION_OWNERSHIP_AUTH.md \
    hat/hatTopology/c153d_partition_ownership_auth.go \
    hat/hatTopology/c153d_partition_ownership_auth_test.go \
    scripts/format-c153d-ownership-auth.sh \
    scripts/test-c153d-ownership-auth.sh \
    scripts/benchmark-c153d-ownership-auth.sh \
    scripts/test-c153d-package.sh \
    scripts/race-c153d-package.sh \
    scripts/vet-c153d-package.sh \
    scripts/stage-c153d-ownership-auth.sh \
    scripts/commit-c153d-ownership-auth.sh \
    scripts/push-c153d-ownership-auth.sh

git diff --cached --check
git diff --cached --name-only
