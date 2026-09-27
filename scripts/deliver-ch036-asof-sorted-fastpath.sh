#!/usr/bin/env bash
set -euo pipefail

feature_files=(
	CH036_ASOF_SORTED_FASTPATH.md
	hat/hatSql/asof_join.go
	hat/hatSql/ch036_asof_sorted_fastpath_test.go
	scripts/benchmark-ch036-asof-sorted-fastpath.sh
	scripts/deliver-ch036-asof-sorted-fastpath.sh
	scripts/format-ch036-asof-sorted-fastpath.sh
	scripts/test-ch036-asof-sorted-fastpath.sh
	scripts/verify-ch036-asof-sorted-fastpath.sh
)

if ! git diff --cached --quiet; then
	echo "refusing to stage: the index already contains changes" >&2
	exit 1
fi

for path in "${feature_files[@]}"; do
	if [[ ! -e "$path" ]]; then
		echo "missing feature path: $path" >&2
		exit 1
	fi
done

if ! rg -q '^test-ch036-asof-sorted-fastpath:' Makefile || \
	! rg -q '^benchmark-ch036-asof-sorted-fastpath:' Makefile || \
	! rg -q '^format-ch036-asof-sorted-fastpath:' Makefile || \
	! rg -q '^verify-ch036-asof-sorted-fastpath:' Makefile; then
	echo "expected CH036 Makefile targets are missing" >&2
	exit 1
fi

git add -- "${feature_files[@]}"

makefile_snapshot="$(mktemp /tmp/hatrie-ch036-makefile.XXXXXX)"
cleanup() {
	rm -f "$makefile_snapshot"
}
trap cleanup EXIT
git show HEAD:Makefile > "$makefile_snapshot"
printf '\n' >> "$makefile_snapshot"
printf '%s\n' \
	'test-ch036-asof-sorted-fastpath:' \
	$'\t@bash scripts/test-ch036-asof-sorted-fastpath.sh' \
	'' \
	'benchmark-ch036-asof-sorted-fastpath:' \
	$'\t@bash scripts/benchmark-ch036-asof-sorted-fastpath.sh' \
	'' \
	'format-ch036-asof-sorted-fastpath:' \
	$'\t@bash scripts/format-ch036-asof-sorted-fastpath.sh' \
	'' \
	'verify-ch036-asof-sorted-fastpath:' \
	$'\t@bash scripts/verify-ch036-asof-sorted-fastpath.sh' \
	'' \
	'deliver-ch036-asof-sorted-fastpath:' \
	$'\t@bash scripts/deliver-ch036-asof-sorted-fastpath.sh' \
	>> "$makefile_snapshot"

makefile_mode="$(git ls-files -s Makefile | awk 'NR == 1 {print $1}')"
if [[ -z "$makefile_mode" ]]; then
	echo "could not determine Makefile index mode" >&2
	exit 1
fi
makefile_blob="$(git hash-object -w "$makefile_snapshot")"
git update-index --add --cacheinfo "$makefile_mode,$makefile_blob,Makefile"

allowed_paths=(
	CH036_ASOF_SORTED_FASTPATH.md
	Makefile
	hat/hatSql/asof_join.go
	hat/hatSql/ch036_asof_sorted_fastpath_test.go
	scripts/benchmark-ch036-asof-sorted-fastpath.sh
	scripts/deliver-ch036-asof-sorted-fastpath.sh
	scripts/format-ch036-asof-sorted-fastpath.sh
	scripts/test-ch036-asof-sorted-fastpath.sh
	scripts/verify-ch036-asof-sorted-fastpath.sh
)
while IFS= read -r path; do
	case " ${allowed_paths[*]} " in
		*" $path "*) ;;
		*) echo "unexpected staged path: $path" >&2; exit 1 ;;
	esac
done < <(git diff --cached --name-only)

git diff --cached --check
git commit -m 'perf: skip redundant ASOF JOIN sorting [skip ci]'
git push origin HEAD
