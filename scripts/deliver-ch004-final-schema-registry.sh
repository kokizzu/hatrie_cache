#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	echo "refusing delivery: the index already contains staged changes" >&2
	exit 1
fi

tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch004-final-schema-registry-delivery.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

git show HEAD:Makefile > "$tmp_dir/Makefile"
printf '%s\n' \
	"" \
	".PHONY: benchmark-ch004-final-schema-registry" \
	"benchmark-ch004-final-schema-registry:" \
	$'\tbash ./scripts/benchmark-ch004-final-schema-registry.sh' \
	"" \
	".PHONY: test-ch004-final-schema-registry" \
	"test-ch004-final-schema-registry:" \
	$'\tbash ./scripts/test-ch004-final-schema-registry.sh' \
	"" \
	".PHONY: format-ch004-final-schema-registry" \
	"format-ch004-final-schema-registry:" \
	$'\tbash ./scripts/format-ch004-final-schema-registry.sh' \
	"" \
	".PHONY: race-ch004-final-schema-registry" \
	"race-ch004-final-schema-registry:" \
	$'\tbash ./scripts/race-ch004-final-schema-registry.sh' \
	"" \
	".PHONY: vet-ch004-final-schema-registry" \
	"vet-ch004-final-schema-registry:" \
	$'\tbash ./scripts/vet-ch004-final-schema-registry.sh' \
	"" \
	".PHONY: test-ch004-final-schema-registry-package" \
	"test-ch004-final-schema-registry-package:" \
	$'\tbash ./scripts/test-ch004-final-schema-registry-package.sh' \
	>> "$tmp_dir/Makefile"

feature_paths=(
	CH004_FINAL_SCHEMA_REGISTRY.md
	ENGINE_IDEAS.md
	hat/hatSql/ch004_final_schema_registry.go
	hat/hatSql/ch004_final_schema_registry_baseline_benchmark_test.go
	hat/hatSql/ch004_final_schema_registry_benchmark_test.go
	hat/hatSql/ch004_final_schema_registry_test.go
	scripts/benchmark-ch004-final-schema-registry.sh
	scripts/deliver-ch004-final-schema-registry.sh
	scripts/format-ch004-final-schema-registry.sh
	scripts/race-ch004-final-schema-registry.sh
	scripts/test-ch004-final-schema-registry-package.sh
	scripts/test-ch004-final-schema-registry.sh
	scripts/vet-ch004-final-schema-registry.sh
)

git add -- "${feature_paths[@]}"
makefile_blob="$(git hash-object -w -- "$tmp_dir/Makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add FINAL schema registry [skip ci]"
git push origin HEAD
