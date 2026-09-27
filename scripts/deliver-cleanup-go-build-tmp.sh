#!/usr/bin/env bash
set -euo pipefail

feature_files=(
	CLEANUP_GO_BUILD_TMP.md
	scripts/cleanup-go-build-tmp.sh
	scripts/deliver-cleanup-go-build-tmp.sh
	scripts/test-cleanup-go-build-tmp.sh
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

if ! rg -q '^inspect-go-build-tmp:' Makefile || \
	! rg -q '^cleanup-go-build-tmp-preview:' Makefile || \
	! rg -q '^cleanup-go-build-tmp-apply:' Makefile || \
	! rg -q '^test-cleanup-go-build-tmp:' Makefile; then
	echo "expected Go build cleanup Makefile targets are missing" >&2
	exit 1
fi

git add -- "${feature_files[@]}"

makefile_snapshot="$(mktemp /tmp/hatrie-go-build-makefile.XXXXXX)"
cleanup() {
	rm -f "$makefile_snapshot"
}
trap cleanup EXIT
git show HEAD:Makefile > "$makefile_snapshot"
printf '\n' >> "$makefile_snapshot"
printf '%s\n' \
	'inspect-go-build-tmp:' \
	$'\t@bash scripts/cleanup-go-build-tmp.sh preview' \
	'' \
	'cleanup-go-build-tmp-preview:' \
	$'\t@bash scripts/cleanup-go-build-tmp.sh preview' \
	'' \
	'cleanup-go-build-tmp-apply:' \
	$'\t@bash scripts/cleanup-go-build-tmp.sh apply' \
	'' \
	'test-cleanup-go-build-tmp:' \
	$'\t@bash scripts/test-cleanup-go-build-tmp.sh' \
	'' \
	'deliver-cleanup-go-build-tmp:' \
	$'\t@bash scripts/deliver-cleanup-go-build-tmp.sh' \
	>> "$makefile_snapshot"

makefile_mode="$(git ls-files -s Makefile | awk 'NR == 1 {print $1}')"
if [[ -z "$makefile_mode" ]]; then
	echo "could not determine Makefile index mode" >&2
	exit 1
fi
makefile_blob="$(git hash-object -w "$makefile_snapshot")"
git update-index --add --cacheinfo "$makefile_mode,$makefile_blob,Makefile"

allowed_paths=(
	CLEANUP_GO_BUILD_TMP.md
	Makefile
	scripts/cleanup-go-build-tmp.sh
	scripts/deliver-cleanup-go-build-tmp.sh
	scripts/test-cleanup-go-build-tmp.sh
)
while IFS= read -r path; do
	case " ${allowed_paths[*]} " in
		*" $path "*) ;;
		*) echo "unexpected staged path: $path" >&2; exit 1 ;;
	esac
done < <(git diff --cached --name-only)

git diff --cached --check
git commit -m 'chore: add safe Go build temp cleanup [skip ci]'
git push origin HEAD
