#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

if [[ -n "$(git diff --cached --name-only)" ]]; then
    printf '%s\n' 'Refusing to mix pre-existing staged changes with Hatrie temporary-audit delivery.' >&2
    exit 1
fi

git add -- scripts/inspect-hatrie-tmp.sh scripts/deliver-hatrie-tmp-audit.sh

makefile_patch=$(mktemp)
trap 'rm -f -- "$makefile_patch"' EXIT
cat >"$makefile_patch" <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -0,0 +1,7 @@
+.PHONY: inspect-hatrie-tmp-all
+inspect-hatrie-tmp-all:
+	bash ./scripts/inspect-hatrie-tmp.sh
+
+.PHONY: deliver-hatrie-tmp-audit
+deliver-hatrie-tmp-audit:
+	bash ./scripts/deliver-hatrie-tmp-audit.sh
PATCH
git apply --cached --unidiff-zero "$makefile_patch"

expected=(Makefile scripts/deliver-hatrie-tmp-audit.sh scripts/inspect-hatrie-tmp.sh)
mapfile -t staged < <(git diff --cached --name-only)
if (( ${#staged[@]} != ${#expected[@]} )); then
    printf 'Unexpected staged path count: got %s, want %s\n' "${#staged[@]}" "${#expected[@]}" >&2
    git diff --cached --name-only >&2
    exit 1
fi
for path in "${expected[@]}"; do
    found=0
    for staged_path in "${staged[@]}"; do
        if [[ "$staged_path" == "$path" ]]; then
            found=1
            break
        fi
    done
    if (( found == 0 )); then
        printf 'Missing expected staged path: %s\n' "$path" >&2
        exit 1
    fi
done

git diff --cached --check
git commit -m 'chore: audit Hatrie temporary build artifacts [skip ci]'
git push origin HEAD
