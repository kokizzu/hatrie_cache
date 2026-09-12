#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

git fetch origin master
base_revision=$(git rev-parse origin/master)
worktree_parent=$(mktemp -d /tmp/hatrie-cache-t-u49.XXXXXX)
worktree="$worktree_parent/worktree"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$worktree_parent"
}
trap cleanup EXIT
git worktree add --detach "$worktree" "$base_revision"

feature_files=(
	REPLICA_HEDGING.md
	hat/hatTopology/replica_hedging.go
	hat/hatTopology/replica_hedging_test.go
	hat/hatTopology/replica_hedging_benchmark_test.go
	scripts/benchmark-t-u49-baseline.sh
	scripts/benchmark-t-u49.sh
	scripts/format-t-u49.sh
	scripts/publish-t-u49.sh
	scripts/test-t-u49.sh
	scripts/verify-t-u49.sh
)
for path in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname -- "$path")"
	cp "$repo_root/$path" "$worktree/$path"
done

if ! rg -q '^benchmark-t-u49-baseline:' "$worktree/Makefile"; then
	cat >>"$worktree/Makefile" <<'MAKE'

benchmark-t-u49-baseline:
	bash ./scripts/benchmark-t-u49-baseline.sh

test-t-u49:
	bash ./scripts/test-t-u49.sh

format-t-u49:
	bash ./scripts/format-t-u49.sh

benchmark-t-u49:
	bash ./scripts/benchmark-t-u49.sh

verify-t-u49:
	bash ./scripts/verify-t-u49.sh
MAKE
fi

python3 - "$worktree/README.md" "$worktree/PRODUCT_IDEA_GAPS.md" <<'PY'
from pathlib import Path
import sys

readme_path = Path(sys.argv[1])
catalog_path = Path(sys.argv[2])

readme = readme_path.read_text()
readme_marker = "- Opt-in method-aware peer retries: [RETRY_POLICY.md](RETRY_POLICY.md)"
readme_entry = "- Opt-in bounded replica read hedging: [REPLICA_HEDGING.md](REPLICA_HEDGING.md)"
if readme_entry not in readme:
    if readme_marker not in readme:
        raise SystemExit("README retry-policy anchor not found")
    readme = readme.replace(readme_marker, readme_marker + "\n" + readme_entry, 1)
    readme_path.write_text(readme)

catalog = catalog_path.read_text()
old = "| T-U49 | Replica-set request hedging | Read routing selects candidates, but there is no bounded hedged-read policy that cancels slower replicas. | Tail-latency versus duplicate load and consistency. |"
new = "| T-U49 | Replica-set request hedging | `hatTopology.ExecuteReplicaHedged` now provides opt-in bounded read hedging with delayed fallback, first-success cancellation, deterministic failures, observer events, and a zero-allocation single-candidate fast path. | Tail-latency versus duplicate load and consistency. |"
if old in catalog:
    catalog_path.write_text(catalog.replace(old, new, 1))
elif new not in catalog:
    raise SystemExit("T-U49 catalog row not found")
PY

gofmt -w "$worktree/hat/hatTopology/replica_hedging.go" "$worktree/hat/hatTopology/replica_hedging_test.go" "$worktree/hat/hatTopology/replica_hedging_benchmark_test.go"
make -C "$worktree" format-t-u49
make -C "$worktree" test-t-u49
go test -C "$worktree" -race ./hat/hatTopology -count=1
go vet -C "$worktree" ./hat/hatTopology
make -C "$worktree" benchmark-t-u49
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check
git -C "$worktree" add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(hatTopology): add bounded replica read hedging"
git -C "$worktree" push origin HEAD:master
printf 'published %s\n' "$(git -C "$worktree" rev-parse HEAD)"
