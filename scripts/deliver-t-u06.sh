#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md TU006_REPLICA_READ_ONLY.md \
	hat/hatCache/replica_read_only.go \
	hat/hatCache/tu06_replica_read_only_test.go \
	hat/hatCache/tu06_replica_read_only_benchmark_test.go \
	hat/hatCache/atomic_command.go \
	hat/hatCache/command.go \
	hat/hatCache/conditional.go \
	hat/hatCache/cuckoo_filter.go \
	hat/hatCache/grpc_scalar_batch.go \
	hat/hatCache/grpc_structured_batch.go \
	hat/hatCache/local_partition.go \
	hat/hatCache/main.go \
	hat/hatCache/priority_queue.go \
	hat/hatCache/radix_tree.go \
	hat/hatCache/roaring_bitmap.go \
	hat/hatCache/sql.go \
	hat/hatCache/sql_transaction.go \
	hat/hatCache/sparse_bitset.go \
	hat/hatCache/xor_filter.go \
	scripts/test-t-u06.sh scripts/deliver-t-u06.sh
git diff --cached --check
git commit -m 'feat(cache): add replica-wide read-only enforcement [skip ci]'
git push origin HEAD
