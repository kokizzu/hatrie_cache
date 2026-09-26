#!/usr/bin/env bash
set -euo pipefail

mode="${1:-verify}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

inspiration_row='- [x] T047h gRPC cluster-write phase transport. `CacheService.ClusterWriteCommit` exposes authenticated prepare/commit/abort phases over an opt-in participant endpoint, and `ExecuteClusterWriteCommitOverGRPC` reuses one caller-owned client per participant; TLS, dialing, automatic command wiring, and connection cleanup remain caller-owned. See [T047H_GRPC_PHASE_TRANSPORT.md](T047H_GRPC_PHASE_TRANSPORT.md).'

feature_paths=(
	BENCHMARK.md
	T047H_GRPC_PHASE_TRANSPORT.md
	hat/hatCache/grpc.go
	hat/hatCache/grpc_cluster_write_commit.go
	hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go
	hat/hatCache/tu47_cluster_write_commit_grpc_test.go
	internal/gen/hatriecache/v1/cache.pb.go
	internal/gen/hatriecache/v1/cache_grpc.pb.go
	proto/hatriecache/v1/cache.proto
	scripts/deliver-t047-grpc-transport.sh
	scripts/test-t047-grpc-transport.sh
)

makefile_block() {
	printf '%s\n' \
		'' \
		'.PHONY: test-t047-grpc-transport' \
		'test-t047-grpc-transport:' \
		'\tbash ./scripts/test-t047-grpc-transport.sh' \
		'' \
		'.PHONY: race-t047-grpc-transport' \
		'race-t047-grpc-transport:' \
		'\tbash ./scripts/test-t047-grpc-transport.sh race' \
		'' \
		'.PHONY: test-t047-grpc-transport-package' \
		'test-t047-grpc-transport-package:' \
		'\tbash ./scripts/test-t047-grpc-transport.sh package' \
		'' \
		'.PHONY: format-t047-grpc-transport' \
		'format-t047-grpc-transport:' \
		'\tbash ./scripts/test-t047-grpc-transport.sh format' \
		'' \
		'.PHONY: benchmark-t047-grpc-transport' \
		'benchmark-t047-grpc-transport:' \
		'\tbash ./scripts/test-t047-grpc-transport.sh benchmark' \
		'' \
		'.PHONY: stage-t047-grpc-transport commit-t047-grpc-transport push-t047-grpc-transport' \
		'stage-t047-grpc-transport:' \
		'\tbash ./scripts/deliver-t047-grpc-transport.sh stage' \
		'' \
		'commit-t047-grpc-transport:' \
		'\tbash ./scripts/deliver-t047-grpc-transport.sh commit' \
		'' \
		'push-t047-grpc-transport:' \
		'\tbash ./scripts/deliver-t047-grpc-transport.sh push'
}

stage_clean_overlay() {
	tmp_dir="$(mktemp -d)"
	trap 'rm -rf "$tmp_dir"' RETURN

	git show HEAD:Makefile > "$tmp_dir/Makefile"
	makefile_block >> "$tmp_dir/Makefile"
	makefile_blob="$(git hash-object -w "$tmp_dir/Makefile")"
	git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

	git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
	awk -v row="$inspiration_row" '
		index($0, "- [x] T047g Durable participant state files.") == 1 {
			print
			print row
			found++
			next
		}
		{ print }
		END { if (found != 1) exit 1 }
	' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.next"
	inspiration_blob="$(git hash-object -w "$tmp_dir/INSPIRATION.next")"
	git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md
}

verify_staged_paths() {
	git diff --cached --check
	while IFS= read -r path; do
		case "$path" in
			BENCHMARK.md|INSPIRATION.md|Makefile|T047H_GRPC_PHASE_TRANSPORT.md|hat/hatCache/grpc.go|hat/hatCache/grpc_cluster_write_commit.go|hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go|hat/hatCache/tu47_cluster_write_commit_grpc_test.go|internal/gen/hatriecache/v1/cache.pb.go|internal/gen/hatriecache/v1/cache_grpc.pb.go|proto/hatriecache/v1/cache.proto|scripts/deliver-t047-grpc-transport.sh|scripts/test-t047-grpc-transport.sh) ;;
			*) printf 'unexpected staged path: %s\n' "$path" >&2; exit 1 ;;
		esac
	done < <(git diff --cached --name-only)
}

reset_feature_index() {
	git reset --quiet -- "${feature_paths[@]}" Makefile INSPIRATION.md
}

case "$mode" in
	stage)
		if ! git diff --cached --quiet; then
			printf '%s\n' 'refusing to stage over existing index changes' >&2
			exit 1
		fi
		git add -- "${feature_paths[@]}"
		stage_clean_overlay
		verify_staged_paths
		git diff --cached --stat
		;;
	restage)
		reset_feature_index
		git add -- "${feature_paths[@]}"
		stage_clean_overlay
		verify_staged_paths
		git diff --cached --stat
		;;
	verify)
		verify_staged_paths
		git diff --cached --name-status
		;;
	commit)
		verify_staged_paths
		git diff --cached --quiet && { printf '%s\n' 'nothing staged'; exit 1; }
		git commit -m 'feat(replication): add gRPC cluster write phases [skip ci]'
		;;
	push)
		git push origin HEAD
		;;
	*)
		printf 'usage: %s {stage|restage|verify|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
