#!/usr/bin/env bash
set -euo pipefail

mode=${1:-deliver}
commit_message='feat(replication): add HTTP cluster write phase transport [skip ci]'

feature_files=(
  BENCHMARK.md
  INSPIRATION.md
  Makefile
  T047J_HTTP_PHASE_TRANSPORT.md
  hat/hatCache/http_cluster_write_commit.go
  hat/hatCache/tu47_cluster_write_commit_http_test.go
  hat/hatCache/tu47_cluster_write_commit_http_benchmark_test.go
  scripts/benchmark-t047j-http-transport.sh
  scripts/deliver-t047j-http-transport.sh
  scripts/format-t047j-http-transport.sh
  scripts/race-t047j-http-transport.sh
  scripts/test-t047j-http-transport.sh
  scripts/test-t047j-package.sh
)

makefile_block=$(printf '%s\n' \
  '.PHONY: test-t047j-http-transport' \
  'test-t047j-http-transport:' \
  $'\tbash ./scripts/test-t047j-http-transport.sh' \
  '.PHONY: format-t047j-http-transport' \
  'format-t047j-http-transport:' \
  $'\tbash ./scripts/format-t047j-http-transport.sh' \
  '.PHONY: benchmark-t047j-http-transport' \
  'benchmark-t047j-http-transport:' \
  $'\tbash ./scripts/benchmark-t047j-http-transport.sh' \
  '.PHONY: race-t047j-http-transport' \
  'race-t047j-http-transport:' \
  $'\tbash ./scripts/race-t047j-http-transport.sh' \
  '.PHONY: test-t047j-package' \
  'test-t047j-package:' \
  $'\tbash ./scripts/test-t047j-package.sh' \
  '.PHONY: plan-t047j-http-transport' \
  'plan-t047j-http-transport:' \
  $'\tbash ./scripts/deliver-t047j-http-transport.sh plan' \
  '.PHONY: stage-t047j-http-transport' \
  'stage-t047j-http-transport:' \
  $'\tbash ./scripts/deliver-t047j-http-transport.sh stage' \
  '.PHONY: commit-t047j-http-transport' \
  'commit-t047j-http-transport:' \
  $'\tbash ./scripts/deliver-t047j-http-transport.sh commit' \
  '.PHONY: push-t047j-http-transport' \
  'push-t047j-http-transport:' \
  $'\tbash ./scripts/deliver-t047j-http-transport.sh push' \
  '.PHONY: deliver-t047j-http-transport' \
  'deliver-t047j-http-transport:' \
  $'\tbash ./scripts/deliver-t047j-http-transport.sh deliver')

inspiration_line='- [x] T047j Opt-in HTTP prepare/commit/abort phase transport. `ClusterWriteCommitHTTPHandler` and `ExecuteClusterWriteCommitOverHTTP` provide strict bounded JSON requests, constant-time token authentication, and caller-owned HTTP/TLS lifecycle without registering a default route; see [T047J_HTTP_PHASE_TRANSPORT.md](T047J_HTTP_PHASE_TRANSPORT.md).'

benchmark_section=$(printf '%s\n' \
  '## T047j HTTP phase transport' \
  '' \
  'Measured with `make benchmark-t047j-http-transport`. The HTTP row uses one' \
  'in-process `httptest` participant and executes the complete three-phase' \
  'coordinator. The direct row uses the same coordinator and one direct callback' \
  'participant. Each row was run three times with `-benchmem`.' \
  '' \
  '```text' \
  'goos: linux' \
  'goarch: amd64' \
  'pkg: hatrie_cache/hat/hatCache' \
  'cpu: AMD Ryzen 9 5950X 16-Core Processor' \
  'BenchmarkTU047HTTPTransport-32                6270  163906 ns/op  28908 B/op 254 allocs/op' \
  'BenchmarkTU047HTTPTransport-32                7119  159485 ns/op  29007 B/op 254 allocs/op' \
  'BenchmarkTU047HTTPTransport-32                7311  156943 ns/op  28927 B/op 254 allocs/op' \
  'BenchmarkTU047DirectCoordinatorBaseline-32  867777    1417 ns/op    625 B/op  13 allocs/op' \
  'BenchmarkTU047DirectCoordinatorBaseline-32  751207    1442 ns/op    625 B/op  13 allocs/op' \
  'BenchmarkTU047DirectCoordinatorBaseline-32  811896    1466 ns/op    625 B/op  13 allocs/op' \
  'PASS' \
  '```' \
  '' \
  'Median HTTP cost is 159,485 ns/op, 28,927 B/op, and 254 allocs/op. Median' \
  'direct cost is 1,442 ns/op, 625 B/op, and 13 allocs/op. HTTP is therefore' \
  '110.60x slower, uses 46.28x more bytes, and performs 19.54x as many' \
  'allocations. This is an explicit interoperability cost; the transport is' \
  'opt-in and the existing direct and gRPC paths remain unchanged.')

tmp_files=()
cleanup() {
  if ((${#tmp_files[@]} > 0)); then
    rm -f "${tmp_files[@]}"
  fi
}
trap cleanup EXIT

new_temp() {
  local path
  path=$(mktemp)
  tmp_files+=("$path")
  printf '%s\n' "$path"
}

apply_generated_patch() {
  local path=$1
  local mode=$2
  local addition=$3
  local base modified patch status
  base=$(new_temp)
  modified=$(new_temp)
  patch=$(new_temp)
  git show "HEAD:$path" > "$base"
  case "$mode" in
  append|makefile)
    cp "$base" "$modified"
    printf '\n%s\n' "$addition" >> "$modified"
    ;;
  inspiration)
    awk -v insert="$addition" '
      /^- \[x\] T048 Replication sets and peer topology\./ && !inserted {
        print insert
        inserted = 1
      }
      { print }
      END {
        if (!inserted) exit 1
      }
    ' "$base" > "$modified"
    ;;
  *)
    printf 'unsupported patch mode: %s\n' "$mode" >&2
    return 1
    ;;
  esac
  if diff -u --label "a/$path" --label "b/$path" "$base" "$modified" > "$patch"; then
    printf 'no changes generated for %s\n' "$path" >&2
    return 1
  else
    status=$?
    if [[ $status -ne 1 ]]; then
      return "$status"
    fi
  fi
  git apply --cached --check "$patch"
  git apply --cached "$patch"
}

stage_feature() {
  if ! git diff --cached --quiet; then
    printf 'refusing to stage: index already contains changes\n' >&2
    return 1
  fi
  git add -- \
    T047J_HTTP_PHASE_TRANSPORT.md \
    hat/hatCache/http_cluster_write_commit.go \
    hat/hatCache/tu47_cluster_write_commit_http_test.go \
    hat/hatCache/tu47_cluster_write_commit_http_benchmark_test.go \
    scripts/benchmark-t047j-http-transport.sh \
    scripts/deliver-t047j-http-transport.sh \
    scripts/format-t047j-http-transport.sh \
    scripts/race-t047j-http-transport.sh \
    scripts/test-t047j-http-transport.sh \
    scripts/test-t047j-package.sh
  apply_generated_patch Makefile makefile "$makefile_block"
  apply_generated_patch INSPIRATION.md inspiration "$inspiration_line"
  apply_generated_patch BENCHMARK.md append "$benchmark_section"
  git diff --cached --check
}

verify_staged_paths() {
  local path staged
  staged=$(git diff --cached --name-only)
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    if ! printf '%s\n' "${feature_files[@]}" | grep -Fxq "$path"; then
      printf 'refusing unexpected staged path: %s\n' "$path" >&2
      return 1
    fi
  done <<< "$staged"
  for path in "${feature_files[@]}"; do
    if ! printf '%s\n' "$staged" | grep -Fxq "$path"; then
      printf 'refusing incomplete staged feature, missing: %s\n' "$path" >&2
      return 1
    fi
  done
}

stage_feature_and_verify() {
	if git diff --cached --quiet; then
		stage_feature
	else
		verify_staged_paths
		# Refresh only this delivery script when the caller already staged the
		# feature; shared files remain protected from broad restaging.
		git add -- scripts/deliver-t047j-http-transport.sh
		git diff --cached --check
	fi
	verify_staged_paths
	git diff --cached --stat
}

case "$mode" in
plan)
  printf '%s\n' "${feature_files[@]}"
  printf '%s\n' 'Makefile, INSPIRATION.md, and BENCHMARK.md are staged through generated selective patches.'
  ;;
stage)
  stage_feature_and_verify
  ;;
commit)
  stage_feature_and_verify
  git commit -m "$commit_message"
  ;;
push)
  git push origin HEAD
  ;;
deliver)
  stage_feature_and_verify
  git commit -m "$commit_message"
  git push origin HEAD
  ;;
*)
  printf 'usage: %s [plan|stage|commit|push|deliver]\n' "$0" >&2
  exit 2
  ;;
esac
