#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
gate)
	go test ./hat/hatReplication -run '^TestReplicaReadOnlyGateTransitions$' -count=1
	;;
gate-race)
	go test -race ./hat/hatReplication -run '^TestReplicaReadOnlyGateTransitions$' -count=1
	;;
gate-vet)
	go vet ./hat/hatReplication
	;;
test)
	go test ./hat/hatCache -run '^TestReplicaReadOnlyGate' -count=1
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU06UpsertString(Baseline|WritableGate)$' -benchmem -count=5
	;;
test-package)
	go test ./hat/hatCache -count=1
	;;
race)
	go test -race ./hat/hatCache -run 'ReplicaReadOnly|LocalPartition|Replication' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
format)
	gofmt -w hat/hatReplication/replica_read_only.go hat/hatReplication/replica_read_only_test.go hat/hatCache/atomic_command.go hat/hatCache/bloom_filter.go hat/hatCache/command.go hat/hatCache/conditional.go hat/hatCache/count_min_sketch.go hat/hatCache/cuckoo_filter.go hat/hatCache/fenwick_tree.go hat/hatCache/grpc_scalar_batch.go hat/hatCache/grpc_structured_batch.go hat/hatCache/hyperloglog.go hat/hatCache/integrity.go hat/hatCache/local_partition.go hat/hatCache/main.go hat/hatCache/priority_queue.go hat/hatCache/quantile_sketch.go hat/hatCache/radix_tree.go hat/hatCache/reservoir_sample.go hat/hatCache/roaring_bitmap.go hat/hatCache/snapshot.go hat/hatCache/sparse_bitset.go hat/hatCache/top_k.go hat/hatCache/top_k_aggregate.go hat/hatCache/tu06_replica_read_only_batch_test.go hat/hatCache/tu06_replica_read_only_test.go hat/hatCache/xor_filter.go
	;;
stage)
	git add BENCHMARK.md Makefile PRODUCT_IDEA_GAPS.md README.md TU06_REPLICA_READ_ONLY.md hat/hatCache/atomic_command.go hat/hatCache/bloom_filter.go hat/hatCache/command.go hat/hatCache/conditional.go hat/hatCache/count_min_sketch.go hat/hatCache/cuckoo_filter.go hat/hatCache/fenwick_tree.go hat/hatCache/grpc_scalar_batch.go hat/hatCache/grpc_structured_batch.go hat/hatCache/hyperloglog.go hat/hatCache/integrity.go hat/hatCache/local_partition.go hat/hatCache/main.go hat/hatCache/priority_queue.go hat/hatCache/quantile_sketch.go hat/hatCache/radix_tree.go hat/hatCache/reservoir_sample.go hat/hatCache/roaring_bitmap.go hat/hatCache/snapshot.go hat/hatCache/sparse_bitset.go hat/hatCache/top_k.go hat/hatCache/top_k_aggregate.go hat/hatCache/tu06_replica_read_only_batch_test.go hat/hatCache/tu06_replica_read_only_test.go hat/hatCache/xor_filter.go hat/hatCache/replica_read_only.go hat/hatReplication/replica_read_only.go hat/hatReplication/replica_read_only_test.go scripts/tu06-replica-read-only.sh
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	git commit -m 'feat: enforce replica-wide read-only mutations [skip ci]'
	;;
push)
	git push -u origin codex/inspiration-tg26-next-20261007
	;;
*)
	printf 'usage: %s {gate|gate-race|gate-vet|test|benchmark|test-package|race|vet|format|stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
