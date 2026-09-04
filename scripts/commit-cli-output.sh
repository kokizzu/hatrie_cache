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

commit_message='cli: add opt-in pretty output'
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

feature_targets='format-cli-output|test-cli-output|test-race-cli-output|vet-cli-output|benchmark-cli-output|stage-cli-output|commit-cli-output|push-cli-output'
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
    printf 'refusing to stage an already-committed CLI output Makefile fragment\n' >&2
    exit 1
  fi
else
  if [ ! -s "$fragment" ]; then
    printf 'CLI output Makefile targets are missing\n' >&2
    exit 1
  fi
  cat "$base" "$fragment" > "$desired"
  if git diff --cached --quiet -- Makefile; then
    if diff -u --label a/Makefile --label b/Makefile "$base" "$desired" > "$patch"; then
      printf 'CLI output Makefile targets are already committed\n' >&2
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

git add -- \
  CLI_OUTPUT.md \
  INSPIRATION.md \
  README.md \
  BENCHMARK.md \
  cmd/hatrie-cli/main.go \
  cmd/hatrie-cli/output_format_test.go \
  scripts/benchmark-cli-output.sh \
  scripts/commit-cli-output.sh \
  scripts/format-cli-output.sh \
  scripts/test-cli-output.sh \
  scripts/test-race-cli-output.sh \
  scripts/vet-cli-output.sh

git diff --cached --check
git diff --cached --stat

if [ "$mode" = "commit" ]; then
  git commit -m "$commit_message"
fi
