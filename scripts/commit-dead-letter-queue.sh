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

if [ "$mode" = "push" ]; then
  git push origin HEAD
  exit 0
fi

base=$(mktemp)
fragment=$(mktemp)
desired=$(mktemp)
patch=$(mktemp)
staged=$(mktemp)
trap 'rm -f "$base" "$fragment" "$desired" "$patch" "$staged"' EXIT

feature_targets='test-dead-letter-queue|format-dead-letter-queue|test-race-dead-letter-queue|vet-dead-letter-queue|benchmark-dead-letter-queue|stage-dead-letter-queue|commit-dead-letter-queue|push-dead-letter-queue'
git show HEAD:Makefile > "$base"
head_target_count=$(awk -v targets="$feature_targets" '
/^\.PHONY:/ {
  if ($0 ~ "^\\.PHONY: (" targets ")$") {
    count++
  }
}
END {
  print count + 0
}
' "$base")

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

if [ "$head_target_count" -ge 8 ]; then
  if ! git diff --cached --quiet -- Makefile; then
    printf 'refusing to stage an already-committed dead-letter Makefile fragment\n' >&2
    exit 1
  fi
else
  if [ ! -s "$fragment" ]; then
    printf 'dead-letter Makefile targets are missing\n' >&2
    exit 1
  fi
  cat "$base" "$fragment" > "$desired"
  if git diff --cached --quiet -- Makefile; then
    if diff -u --label a/Makefile --label b/Makefile "$base" "$desired" > "$patch"; then
      printf 'dead-letter Makefile targets are already committed\n' >&2
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
fi

git add -- DEAD_LETTER_QUEUE.md INSPIRATION.md README.md BENCHMARK.md hat/hatDataStructure/dead_letter_queue.go hat/hatDataStructure/dead_letter_queue_test.go hat/hatDataStructure/dead_letter_queue_benchmark_test.go scripts/benchmark-dead-letter-queue.sh scripts/commit-dead-letter-queue.sh scripts/format-dead-letter-queue.sh scripts/test-dead-letter-queue.sh scripts/test-race-dead-letter-queue.sh scripts/vet-dead-letter-queue.sh

git diff --cached --check
git diff --cached --stat

if [ "$mode" = "commit" ]; then
  git commit -m 'data: add bounded dead-letter queue'
fi
