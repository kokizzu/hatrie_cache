#!/bin/sh
set -eu

mode=${1:-stage}
case "$mode" in
stage|commit|push) ;;
*)
  printf 'usage: %s [stage|commit|push]\n' "$0" >&2
  exit 2
  ;;
esac

base=$(mktemp)
fragment=$(mktemp)
desired=$(mktemp)
patch=$(mktemp)
staged=$(mktemp)
trap 'rm -f "$base" "$fragment" "$desired" "$patch" "$staged"' EXIT

feature_targets='test-delay-queue|format-delay-queue|benchmark-delay-queue|test-race-delay-queue|vet-delay-queue|stage-delay-queue|commit-delay-queue|push-delay-queue'

git show HEAD:Makefile > "$base"
awk -v targets="$feature_targets" '
function emit() {
  if (keep) {
    printf "%s", block
  }
}
BEGIN {
  keep = 0
  block = ""
}
/^\.PHONY:/ {
  emit()
  block = $0 "\n"
  keep = ($0 ~ "^\\.PHONY: (" targets ")$")
  next
}
{
  if (block != "") {
    block = block $0 "\n"
  }
}
END {
  emit()
}
' Makefile > "$fragment"

while [ -s "$fragment" ] && [ "$(tail -n 1 "$fragment")" = "" ]; do
  sed -i '$d' "$fragment"
done

if [ ! -s "$fragment" ]; then
  printf 'delay-queue Makefile targets are missing\n' >&2
  exit 1
fi

cat "$base" "$fragment" > "$desired"

if git diff --cached --quiet -- Makefile; then
  if diff -u --label a/Makefile --label b/Makefile "$base" "$desired" > "$patch"; then
    printf 'delay-queue Makefile targets are already committed\n' >&2
    exit 1
  else
    diff_status=$?
    if [ "$diff_status" -ne 1 ]; then
      exit "$diff_status"
    fi
  fi
  git apply --cached "$patch"
else
  git show :Makefile > "$staged"
  if ! cmp -s "$staged" "$desired"; then
    printf 'refusing to replace an unrelated staged Makefile change\n' >&2
    exit 1
  fi
fi

git add -- \
  DELAY_QUEUE.md \
  INSPIRATION.md \
  README.md \
  BENCHMARK.md \
  hat/hatDataStructure/delay_queue.go \
  hat/hatDataStructure/delay_queue_test.go \
  hat/hatDataStructure/delay_queue_benchmark_test.go \
  scripts/benchmark-delay-queue.sh \
  scripts/format-delay-queue.sh \
  scripts/test-delay-queue.sh \
  scripts/test-race-delay-queue.sh \
  scripts/vet-delay-queue.sh \
  scripts/commit-delay-queue.sh

git diff --cached --check
git diff --cached --stat

case "$mode" in
commit)
  git commit -m 'data: add allocation-free delay queue'
  ;;
push)
  git push origin HEAD
  ;;
esac
