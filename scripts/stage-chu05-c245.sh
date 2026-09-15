#!/usr/bin/env bash
set -euo pipefail

allowlist=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU05_EXTERNAL_WINDOW_STREAM.md
	Makefile
	PRODUCT_IDEA_GAPS.md
	README.md
	hat/hatSql/chu05_external_window_stream_benchmark_test.go
	hat/hatSql/chu05_external_window_stream_test.go
	hat/hatSql/query.go
	scripts/benchmark-chu05-c245.sh
	scripts/commit-chu05-c245.sh
	scripts/format-chu05-c245.sh
	scripts/memory-chu05-c245.sh
	scripts/push-chu05-c245.sh
	scripts/race-chu05-c245.sh
	scripts/stage-chu05-c245.sh
	scripts/test-chu05-c245.sh
	scripts/test-chu05-package-c245.sh
	scripts/verify-chu05-docs-c245.sh
	scripts/vet-chu05-c245.sh
)

temporary_root="$(mktemp -d)"
cleanup() {
	rm -rf "$temporary_root"
}
trap cleanup EXIT

is_allowed() {
	local candidate="$1"
	local allowed
	for allowed in "${allowlist[@]}"; do
		if [[ "$candidate" == "$allowed" ]]; then
			return 0
		fi
	done
	return 1
}

cached_paths="$temporary_root/cached-paths"
git diff --cached --name-only > "$cached_paths"
while IFS= read -r path; do
	if [[ -n "$path" ]] && ! is_allowed "$path"; then
		printf 'Refusing to stage: unrelated path is already staged: %s\n' "$path" >&2
		exit 1
	fi
done < "$cached_paths"

for path in "${allowlist[@]}"; do
	if [[ "$path" != "Makefile" ]] && [[ ! -e "$path" ]]; then
		printf 'Required CH-U05 path is missing: %s\n' "$path" >&2
		exit 1
	fi
done

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CHU05_EXTERNAL_WINDOW_STREAM.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatSql/chu05_external_window_stream_benchmark_test.go \
	hat/hatSql/chu05_external_window_stream_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-chu05-c245.sh \
	scripts/commit-chu05-c245.sh \
	scripts/format-chu05-c245.sh \
	scripts/memory-chu05-c245.sh \
	scripts/push-chu05-c245.sh \
	scripts/race-chu05-c245.sh \
	scripts/stage-chu05-c245.sh \
	scripts/test-chu05-c245.sh \
	scripts/test-chu05-package-c245.sh \
	scripts/verify-chu05-docs-c245.sh \
	scripts/vet-chu05-c245.sh

base_makefile="$temporary_root/Makefile.base"
desired_makefile="$temporary_root/Makefile.desired"
target_block="$temporary_root/Makefile.targets"
patch_file="$temporary_root/Makefile.patch"
index_makefile="$temporary_root/Makefile.index"
git show HEAD:Makefile > "$base_makefile"
cat > "$target_block" <<'EOF'
.PHONY: test-chu05-c245
test-chu05-c245:
	@bash ./scripts/test-chu05-c245.sh

.PHONY: format-chu05-c245
format-chu05-c245:
	@bash ./scripts/format-chu05-c245.sh

.PHONY: benchmark-chu05-c245
benchmark-chu05-c245:
	@bash ./scripts/benchmark-chu05-c245.sh

.PHONY: memory-chu05-c245
memory-chu05-c245:
	@bash ./scripts/memory-chu05-c245.sh

.PHONY: test-chu05-package-c245
test-chu05-package-c245:
	@bash ./scripts/test-chu05-package-c245.sh

.PHONY: race-chu05-c245
race-chu05-c245:
	@bash ./scripts/race-chu05-c245.sh

.PHONY: vet-chu05-c245
vet-chu05-c245:
	@bash ./scripts/vet-chu05-c245.sh

.PHONY: verify-chu05-docs-c245
verify-chu05-docs-c245:
	@bash ./scripts/verify-chu05-docs-c245.sh

.PHONY: stage-chu05-c245
stage-chu05-c245:
	@bash ./scripts/stage-chu05-c245.sh

.PHONY: commit-chu05-c245
commit-chu05-c245:
	@bash ./scripts/commit-chu05-c245.sh

.PHONY: push-chu05-c245
push-chu05-c245:
	@bash ./scripts/push-chu05-c245.sh
EOF
awk -v block="$target_block" '
{
	print
	if (!inserted && $0 == "\t@bash ./scripts/push-chu04-c243.sh") {
		while ((getline line < block) > 0) {
			print line
		}
		close(block)
		inserted = 1
	}
}
END {
	if (!inserted) {
		exit 1
	}
}
' "$base_makefile" > "$desired_makefile"

needs_makefile_patch=0
git show :Makefile > "$index_makefile"
for target in test-chu05-c245 format-chu05-c245 benchmark-chu05-c245 memory-chu05-c245 test-chu05-package-c245 race-chu05-c245 vet-chu05-c245 verify-chu05-docs-c245 stage-chu05-c245 commit-chu05-c245 push-chu05-c245; do
	if ! rg -q "^${target}:" "$index_makefile"; then
		needs_makefile_patch=1
	fi
done
if [[ "$needs_makefile_patch" -eq 1 ]]; then
	set +e
	diff -u --label a/Makefile --label b/Makefile "$base_makefile" "$desired_makefile" > "$patch_file"
	diff_status=$?
	set -e
	if [[ "$diff_status" -ne 1 ]]; then
		printf 'Unexpected Makefile diff status: %s\n' "$diff_status" >&2
		exit 1
	fi
	git apply --cached "$patch_file"
fi

git diff --cached --check
git diff --cached --name-only > "$cached_paths"
cached_count=0
while IFS= read -r path; do
	if [[ -n "$path" ]]; then
		cached_count=$((cached_count + 1))
		if ! is_allowed "$path"; then
			printf 'Unexpected staged path: %s\n' "$path" >&2
			exit 1
		fi
	fi
done < "$cached_paths"
if [[ "$cached_count" -ne "${#allowlist[@]}" ]]; then
	printf 'Staged %s paths, expected exactly %s CH-U05 paths.\n' "$cached_count" "${#allowlist[@]}" >&2
	exit 1
fi
printf 'CH-U05 staged paths:\n'
cat "$cached_paths"
