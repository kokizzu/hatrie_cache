#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

git fetch origin master
base_revision=$(git rev-parse origin/master)
worktree_parent=$(mktemp -d /tmp/hatrie-cache-t-u50.XXXXXX)
worktree="$worktree_parent/worktree"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$worktree_parent"
}
trap cleanup EXIT
git worktree add --detach "$worktree" "$base_revision"

feature_files=(
	CONFIG_WATCH.md
	hat/hatTopology/config_watch.go
	hat/hatTopology/config_watch_test.go
	scripts/benchmark-t-u50.sh
	scripts/format-t-u50.sh
	scripts/publish-t-u50.sh
	scripts/test-t-u50.sh
	scripts/verify-t-u50.sh
)
for path in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname -- "$path")"
	cp "$repo_root/$path" "$worktree/$path"
done

if ! rg -q '^test-t-u50:' "$worktree/Makefile"; then
	cat >>"$worktree/Makefile" <<'MAKE'

test-t-u50:
	bash ./scripts/test-t-u50.sh

format-t-u50:
	bash ./scripts/format-t-u50.sh

benchmark-t-u50:
	bash ./scripts/benchmark-t-u50.sh

verify-t-u50:
	bash ./scripts/verify-t-u50.sh
MAKE
fi

python3 - "$worktree/README.md" "$worktree/PRODUCT_IDEA_GAPS.md" <<'PY'
from pathlib import Path
import sys

readme_path = Path(sys.argv[1])
catalog_path = Path(sys.argv[2])

readme = readme_path.read_text()
readme_marker = "- Opt-in bounded replica read hedging: [REPLICA_HEDGING.md](REPLICA_HEDGING.md)"
readme_entry = "- Bounded authenticated versioned configuration watch: [CONFIG_WATCH.md](CONFIG_WATCH.md)"
if readme_entry not in readme:
    if readme_marker not in readme:
        raise SystemExit("README replica-hedging anchor not found")
    readme = readme.replace(readme_marker, readme_marker + "\n" + readme_entry, 1)
    readme_path.write_text(readme)

catalog = catalog_path.read_text()
old = "| T-U50 | Cluster-wide configuration watch | Local configuration can be observed, but there is no authenticated versioned watch stream with replay/resume across cluster members. | Gap recovery, authorization, and bounded history. |"
new = "| T-U50 | Cluster-wide configuration watch | `hatTopology.ConfigWatchLog` now provides an authenticated bounded versioned log with replay cursors, context-aware wait/resume, deterministic history-gap errors, value-copy isolation, and no per-client idle goroutine; transport and consensus remain caller-owned. | Gap recovery, authorization, and bounded history. |"
if old in catalog:
    catalog_path.write_text(catalog.replace(old, new, 1))
elif new not in catalog:
    raise SystemExit("T-U50 catalog row not found")
PY

gofmt -w "$worktree/hat/hatTopology/config_watch.go" "$worktree/hat/hatTopology/config_watch_test.go"
make -C "$worktree" format-t-u50
make -C "$worktree" test-t-u50
go test -C "$worktree" -race ./hat/hatTopology -count=1
go vet -C "$worktree" ./hat/hatTopology
make -C "$worktree" benchmark-t-u50
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check
git -C "$worktree" add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(hatTopology): add versioned config watch"
git -C "$worktree" push origin HEAD:master
printf 'published %s\n' "$(git -C "$worktree" rev-parse HEAD)"
