#!/usr/bin/env bash
set -euo pipefail

mode=${1:-plan}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-stage-m052aa.XXXXXX")
trap 'rm -rf -- "$tmp_dir"' EXIT

feature_paths=(
  BENCHMARK.md
  INSPIRATION.md
  Makefile
  M052AA_NATIVE_HASH_JOIN.md
  hat/hatSql/m052aa_auto_native_join.go
  hat/hatSql/m052aa_auto_native_join_test.go
  hat/hatSql/m052p_auto_native_dataflow.go
  scripts/benchmark-m052aa-native-join-baseline.sh
  scripts/benchmark-m052aa-native-join.sh
  scripts/commit-m052aa-native-join.sh
  scripts/format-m052aa-native-join.sh
  scripts/push-m052aa-native-join.sh
  scripts/race-m052aa-native-join.sh
  scripts/stage-m052aa-native-join.sh
  scripts/test-m052aa-native-join-package.sh
  scripts/test-m052aa-native-join.sh
  scripts/vet-m052aa-native-join.sh
)

declare -A expected=()
for path in "${feature_paths[@]}"; do
  expected["$path"]=1
done

verify_index() {
  mapfile -t actual < <(git diff --cached --name-only)
  if (( ${#actual[@]} != ${#feature_paths[@]} )); then
    printf 'staged path count = %d, want %d\n' "${#actual[@]}" "${#feature_paths[@]}" >&2
    printf '%s\n' "${actual[@]}" >&2
    return 1
  fi
  for path in "${actual[@]}"; do
    if [[ -z ${expected["$path"]+present} ]]; then
      printf 'unexpected staged path: %s\n' "$path" >&2
      return 1
    fi
  done
  git diff --cached --check
  printf 'Verified staged paths:\n'
  printf '  %s\n' "${actual[@]}"
}

if [[ "$mode" == "rollback" ]]; then
  git restore --staged -- "${feature_paths[@]}"
  printf '%s\n' 'Removed this feature from the index.'
  exit 0
fi

if [[ "$mode" == "plan" ]]; then
  printf 'Feature staging plan:\n'
  printf '  %s\n' "${feature_paths[@]}"
  printf 'Existing staged paths:\n'
  git diff --cached --name-only
  exit 0
fi

if [[ "$mode" == "verify" ]]; then
  verify_index
  exit 0
fi

if [[ "$mode" != "apply" ]]; then
  printf 'usage: %s [plan|apply|verify]\n' "$0" >&2
  exit 2
fi

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to stage: the index already contains changes' >&2
  git diff --cached --name-only >&2
  exit 1
fi

make_section=$(awk '/^\.PHONY: test-m052aa-native-join / {capture=1} capture {print} capture && /^benchmark-m052aa-native-join:$/ {seen=1} seen && /^$/ {exit}' Makefile)
inspiration_section=$(awk '/^- \[x\] M065t / {if (capture) exit} /^- \[x\] M052aa Automatic safe native equality hash joins\./ {capture=1} capture {print}' INSPIRATION.md)
benchmark_section=$(awk '/^<a id="m052aa-automatic-native-equality-hash-join"><\/a>$/ {capture=1} capture {print}' BENCHMARK.md)
if [[ -z "$make_section" || -z "$inspiration_section" || -z "$benchmark_section" ]]; then
  printf '%s\n' 'feature sections were not found in the working tree' >&2
  exit 1
fi

git add -- \
  M052AA_NATIVE_HASH_JOIN.md \
  hat/hatSql/m052aa_auto_native_join.go \
  hat/hatSql/m052aa_auto_native_join_test.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  scripts/benchmark-m052aa-native-join-baseline.sh \
  scripts/benchmark-m052aa-native-join.sh \
  scripts/commit-m052aa-native-join.sh \
  scripts/format-m052aa-native-join.sh \
  scripts/push-m052aa-native-join.sh \
  scripts/race-m052aa-native-join.sh \
  scripts/stage-m052aa-native-join.sh \
  scripts/test-m052aa-native-join-package.sh \
  scripts/test-m052aa-native-join.sh \
  scripts/vet-m052aa-native-join.sh

git show HEAD:Makefile > "$tmp_dir/Makefile"
printf '\n%s\n' "$make_section" >> "$tmp_dir/Makefile"
cp "$tmp_dir/Makefile" "$tmp_dir/Makefile.next"
make_blob=$(git hash-object -w "$tmp_dir/Makefile.next")
git update-index --add --cacheinfo "100644,$make_blob,Makefile"

git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
printf '\n%s\n' "$inspiration_section" >> "$tmp_dir/INSPIRATION.md"
inspiration_blob=$(git hash-object -w "$tmp_dir/INSPIRATION.md")
git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
printf '\n%s\n' "$benchmark_section" >> "$tmp_dir/BENCHMARK.md"
benchmark_blob=$(git hash-object -w "$tmp_dir/BENCHMARK.md")
git update-index --add --cacheinfo "100644,$benchmark_blob,BENCHMARK.md"

verify_index
