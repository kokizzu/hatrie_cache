#!/usr/bin/env bash
set -euo pipefail

mode=${1:-plan}
files=(
	MZ029_DISK_RESIDENT_INDEX.md
	MZ029_PERSISTED_INDEX.md
	hat/hatDataStructure/spillable_arrangement.go
	hat/hatDataStructure/spillable_arrangement_disk_index.go
	hat/hatDataStructure/mz029_disk_resident_index_benchmark_test.go
	hat/hatDataStructure/mz029_disk_resident_index_test.go
	scripts/benchmark-mz029-persisted-index.sh
	scripts/deliver-mz029-disk-resident-index.sh
)

case "$mode" in
plan)
	printf 'MZ029 disk-resident index delivery paths:\n'
	for path in "${files[@]}"; do
		if [[ -e "$path" ]]; then
			printf '  %s\n' "$path"
		else
			printf '  MISSING %s\n' "$path" >&2
			exit 1
		fi
	done
	git diff --stat -- "${files[@]}"
	git status --short -- "${files[@]}"
	;;
stage)
	git add -- "${files[@]}"
	;;
commit)
	git commit --only "${files[@]}" -m 'feat(spill): add opt-in disk-resident arrangement index [skip ci]'
	;;
push)
	git push origin HEAD
	;;
deliver)
	"$0" plan
	"$0" stage
	"$0" commit
	"$0" push
	;;
*)
	printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
	exit 2
	;;
esac
