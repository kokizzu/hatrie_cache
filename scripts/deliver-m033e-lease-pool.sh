#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

if [[ -n "$(git diff --cached --name-only)" ]]; then
    printf '%s\n' 'Refusing to mix pre-existing staged changes with M033e delivery.' >&2
    exit 1
fi

files=(
    INSPIRATION.md
    M033E_GLOBAL_TIMESTAMP_LEASE_POOL.md
    hat/hatReplication/global_timestamp_lease_pool.go
    hat/hatReplication/global_timestamp_lease_pool_test.go
    hat/hatReplication/global_timestamp_lease_pool_benchmark_test.go
    scripts/benchmark-m033e-lease-pool.sh
    scripts/format-m033e-lease-pool.sh
    scripts/race-m033e-lease-pool.sh
    scripts/test-m033e-lease-pool-package.sh
    scripts/test-m033e-lease-pool.sh
    scripts/vet-m033e-lease-pool.sh
    scripts/deliver-m033e-lease-pool.sh
)
git add -- "${files[@]}"

makefile_patch=$(mktemp)
inspiration_patch=$(mktemp)
trap 'rm -f -- "$makefile_patch" "$inspiration_patch"' EXIT
cat >"$makefile_patch" <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3,0 +4,27 @@
+.PHONY: test-m033e-lease-pool
+test-m033e-lease-pool:
+	bash ./scripts/test-m033e-lease-pool.sh
+
+.PHONY: test-m033e-lease-pool-package
+test-m033e-lease-pool-package:
+	bash ./scripts/test-m033e-lease-pool-package.sh
+
+.PHONY: format-m033e-lease-pool
+format-m033e-lease-pool:
+	bash ./scripts/format-m033e-lease-pool.sh
+
+.PHONY: race-m033e-lease-pool
+race-m033e-lease-pool:
+	bash ./scripts/race-m033e-lease-pool.sh
+
+.PHONY: vet-m033e-lease-pool
+vet-m033e-lease-pool:
+	bash ./scripts/vet-m033e-lease-pool.sh
+
+.PHONY: benchmark-m033e-lease-pool
+benchmark-m033e-lease-pool:
+	bash ./scripts/benchmark-m033e-lease-pool.sh
+
+.PHONY: deliver-m033e-lease-pool
+deliver-m033e-lease-pool:
+	bash ./scripts/deliver-m033e-lease-pool.sh
PATCH
cat >"$inspiration_patch" <<'PATCH'
diff --git a/INSPIRATION.md b/INSPIRATION.md
--- a/INSPIRATION.md
+++ b/INSPIRATION.md
@@ -392,1 +392,2 @@
 - [x] M033d Bounded deterministic binary global timestamp snapshots with CRC32C validation and atomic `0600` file persistence; consensus publication timing and cryptographic authentication remain caller-owned. See [M033D_GLOBAL_TIMESTAMP_SNAPSHOT.md](M033D_GLOBAL_TIMESTAMP_SNAPSHOT.md).
+- [x] M033e Client-side global timestamp range leasing with retry-safe request sequences, bounded default batches, and higher-observation fencing; consensus and coordinator ownership remain caller-owned. See [M033E_GLOBAL_TIMESTAMP_LEASE_POOL.md](M033E_GLOBAL_TIMESTAMP_LEASE_POOL.md).
PATCH
git apply --cached --unidiff-zero "$makefile_patch"
git apply --cached --unidiff-zero "$inspiration_patch"

expected=(
    INSPIRATION.md
    Makefile
    M033E_GLOBAL_TIMESTAMP_LEASE_POOL.md
    hat/hatReplication/global_timestamp_lease_pool.go
    hat/hatReplication/global_timestamp_lease_pool_test.go
    hat/hatReplication/global_timestamp_lease_pool_benchmark_test.go
    scripts/benchmark-m033e-lease-pool.sh
    scripts/deliver-m033e-lease-pool.sh
    scripts/format-m033e-lease-pool.sh
    scripts/race-m033e-lease-pool.sh
    scripts/test-m033e-lease-pool-package.sh
    scripts/test-m033e-lease-pool.sh
    scripts/vet-m033e-lease-pool.sh
)
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
git commit -m 'feat: add global timestamp lease pool [skip ci]'
git push origin HEAD
