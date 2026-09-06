#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

cleanup() {
	git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

git -C "$repo" fetch origin master >/dev/null
git -C "$repo" worktree add --detach "$tmp" origin/master >/dev/null

mkdir -p "$tmp/hat/hatBackup" "$tmp/scripts"
cp "$repo/hat/hatBackup/object_store.go" "$tmp/hat/hatBackup/"
cp "$repo/hat/hatBackup/object_store_verify_test.go" "$tmp/hat/hatBackup/"
cp "$repo/CROSS_REGION_RESTORE_DRILL.md" "$tmp/"
cp "$repo/scripts/inspect-object-backup.sh" "$tmp/scripts/"
cp "$repo/scripts/test-object-store-verify-stage.sh" "$tmp/scripts/"
cp "$repo/scripts/format-object-store-verify.sh" "$tmp/scripts/"
cp "$repo/scripts/verify-object-store-verify.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-object-store-verify.sh" "$tmp/scripts/"

if ! grep -Fq '[x] C157a ' "$tmp/INSPIRATION.md"; then
	awk '
		/C157 / {
			print
			print "- [x] C157a Cross-region backup/restore integrity drill (see CROSS_REGION_RESTORE_DRILL.md)."
			found = 1
			next
		}
		{ print }
		END {
			if (!found) {
				exit 1
			}
		}
	' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.tmp"
	mv "$tmp/INSPIRATION.md.tmp" "$tmp/INSPIRATION.md"
fi

if ! grep -Fq 'CROSS_REGION_RESTORE_DRILL.md' "$tmp/README.md"; then
	printf '\n- [Cross-region restore drill](CROSS_REGION_RESTORE_DRILL.md)\n' >> "$tmp/README.md"
fi

if ! grep -q '^verify-object-store-verify:' "$tmp/Makefile"; then
	printf '\ninspect-object-backup:\n\tsh ./scripts/inspect-object-backup.sh\n\ntest-object-store-verify-stage:\n\tsh ./scripts/test-object-store-verify-stage.sh\n\nformat-object-store-verify:\n\tsh ./scripts/format-object-store-verify.sh\n\nverify-object-store-verify:\n\tsh ./scripts/verify-object-store-verify.sh\n\ndeliver-object-store-verify:\n\tsh ./scripts/deliver-object-store-verify.sh\n' >> "$tmp/Makefile"
fi

cd "$tmp"
go test -run '^TestObjectStoreTargetVerifyChecksEveryPayload$$' ./hat/hatBackup
go test ./hat/hatBackup
go test -race ./hat/hatBackup

git -C "$tmp" add \
	INSPIRATION.md \
	README.md \
	Makefile \
	CROSS_REGION_RESTORE_DRILL.md \
	hat/hatBackup/object_store.go \
	hat/hatBackup/object_store_verify_test.go \
	scripts/inspect-object-backup.sh \
	scripts/test-object-store-verify-stage.sh \
	scripts/format-object-store-verify.sh \
	scripts/verify-object-store-verify.sh \
	scripts/deliver-object-store-verify.sh
git -C "$tmp" commit -m "feat(backup): verify copied object-store bundles"
git -C "$tmp" push origin HEAD:master
