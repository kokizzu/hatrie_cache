#!/usr/bin/env bash
set -euo pipefail

mode=${1:?delivery mode is required}
repo_root=$(pwd)

feature_paths=(
    BENCHMARK.md
    CH048_EXTERNAL_SCHEMA_INFERENCE.md
    INSPIRATION_BACKLOG.md
    README.md
    hat/hatSql/ch048_external_schema_inference_benchmark_test.go
    hat/hatSql/ch048_external_schema_inference_test.go
    hat/hatSql/external_schema.go
    scripts/benchmark-ch048.sh
    scripts/format-ch048.sh
    scripts/test-ch048-red.sh
    scripts/test-ch048.sh
    scripts/verify-ch048.sh
    scripts/deliver-ch048.sh
)

makefile_targets=$(cat <<'EOF'

test-ch048-red:
	bash ./scripts/test-ch048-red.sh

test-ch048:
	bash ./scripts/test-ch048.sh

format-ch048:
	bash ./scripts/format-ch048.sh

benchmark-ch048:
	bash ./scripts/benchmark-ch048.sh

race-ch048:
	bash ./scripts/verify-ch048.sh race

vet-ch048:
	bash ./scripts/verify-ch048.sh vet

verify-ch048:
	bash ./scripts/verify-ch048.sh full

deliver-ch048:
	bash ./scripts/deliver-ch048.sh stage

status-ch048:
	bash ./scripts/deliver-ch048.sh status

review-ch048:
	bash ./scripts/deliver-ch048.sh review

commit-ch048:
	bash ./scripts/deliver-ch048.sh commit

push-ch048:
	bash ./scripts/deliver-ch048.sh push
EOF
)

stage_feature() {
    if ! git diff --cached --quiet --; then
        printf '%s\n' 'refusing to stage CH-48 while the index already contains changes' >&2
        git diff --cached --name-status >&2
        exit 1
    fi
    chmod +x scripts/benchmark-ch048.sh scripts/format-ch048.sh scripts/test-ch048-red.sh scripts/test-ch048.sh scripts/verify-ch048.sh scripts/deliver-ch048.sh
    git add -- "${feature_paths[@]}"

    staged_makefile=$(mktemp /tmp/hatrie-cache-ch048-Makefile.XXXXXX)
    trap 'rm -f "$staged_makefile"' RETURN
    git show HEAD:Makefile > "$staged_makefile"
    printf '%s\n' "$makefile_targets" >> "$staged_makefile"
    makefile_blob=$(git hash-object -w "$staged_makefile")
    git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
    trap - RETURN
    rm -f "$staged_makefile"
}

case "$mode" in
status)
    git status --short
    printf '%s\n' '--- staged files ---'
    git diff --cached --name-status
    ;;
review)
    git diff -- "${feature_paths[@]}"
    ;;
stage)
    stage_feature
    git diff --cached --stat
    ;;
commit)
    stage_feature
    git commit -m '[skip ci] Add bounded external schema inference'
    ;;
push)
    git push origin HEAD:master
    ;;
*)
    printf 'usage: %s {status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
