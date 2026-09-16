#!/usr/bin/env bash
set -euo pipefail

mode=${1:?delivery mode is required}

feature_paths=(
    BENCHMARK.md
    INSPIRATION_BACKLOG.md
    MZ049_SCHEMA_DRIFT_QUARANTINE.md
    README.md
    hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go
    hat/hatSql/mz049_schema_drift_quarantine.go
    hat/hatSql/mz049_schema_drift_quarantine_benchmark_test.go
    hat/hatSql/mz049_schema_drift_quarantine_test.go
    scripts/benchmark-mz049-before.sh
    scripts/benchmark-mz049.sh
    scripts/deliver-mz049.sh
    scripts/format-mz049.sh
    scripts/test-mz049-red.sh
    scripts/test-mz049.sh
    scripts/verify-mz049.sh
)

makefile_targets=$(cat <<'EOF'

test-mz049-red:
	bash ./scripts/test-mz049-red.sh

benchmark-mz049-before:
	bash ./scripts/benchmark-mz049-before.sh

benchmark-mz049:
	bash ./scripts/benchmark-mz049.sh

format-mz049:
	bash ./scripts/format-mz049.sh

test-mz049:
	bash ./scripts/test-mz049.sh

race-mz049:
	bash ./scripts/verify-mz049.sh race

vet-mz049:
	bash ./scripts/verify-mz049.sh vet

verify-mz049:
	bash ./scripts/verify-mz049.sh full

deliver-mz049:
	bash ./scripts/deliver-mz049.sh stage

status-mz049:
	bash ./scripts/deliver-mz049.sh status

review-mz049:
	bash ./scripts/deliver-mz049.sh review

commit-mz049:
	bash ./scripts/deliver-mz049.sh commit

push-mz049:
	bash ./scripts/deliver-mz049.sh push
EOF
)

stage_feature() {
    if ! git diff --cached --quiet --; then
        printf '%s\n' 'refusing to stage MZ-49 while the index already contains changes' >&2
        git diff --cached --name-status >&2
        exit 1
    fi
    chmod +x scripts/benchmark-mz049-before.sh scripts/benchmark-mz049.sh scripts/format-mz049.sh scripts/test-mz049-red.sh scripts/test-mz049.sh scripts/verify-mz049.sh scripts/deliver-mz049.sh
    git add -- "${feature_paths[@]}"

    staged_makefile=$(mktemp /tmp/hatrie-cache-mz049-Makefile.XXXXXX)
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
    git commit -m '[skip ci] Add schema-drift quarantine'
    ;;
push)
    git push origin HEAD:master
    ;;
*)
    printf 'usage: %s {status|review|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
