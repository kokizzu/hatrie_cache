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

mkdir -p "$tmp/hat/hatReplication" "$tmp/scripts"
cp "$repo/hat/hatReplication/quorum_policy_read.go" "$tmp/hat/hatReplication/"
cp "$repo/hat/hatReplication/quorum_policy_read_test.go" "$tmp/hat/hatReplication/"
cp "$repo/hat/hatReplication/quorum_policy_read_edge_test.go" "$tmp/hat/hatReplication/"
cp "$repo/QUORUM_POLICY.md" "$tmp/"
cp "$repo/scripts/format-t058.sh" "$tmp/scripts/"
cp "$repo/scripts/test-t058-stage.sh" "$tmp/scripts/"
cp "$repo/scripts/test-t058-edge.sh" "$tmp/scripts/"
cp "$repo/scripts/verify-t058.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-t058.sh" "$tmp/scripts/"

if ! grep -Fq '[x] T058a ' "$tmp/INSPIRATION.md"; then
	awk '
		/T058 / {
			print
			print "- [x] T058a Validated read/write quorum policy decisions with majority defaults (see QUORUM_POLICY.md)."
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

if ! grep -Fq 'QUORUM_POLICY.md' "$tmp/README.md"; then
	printf '\n- [Quorum policy](QUORUM_POLICY.md)\n' >> "$tmp/README.md"
fi

if ! grep -q '^format-t058:' "$tmp/Makefile"; then
	printf '\nformat-t058:\n\tsh ./scripts/format-t058.sh\n\ntest-t058-stage:\n\tsh ./scripts/test-t058-stage.sh\n\ntest-t058-edge:\n\tsh ./scripts/test-t058-edge.sh\n\nverify-t058:\n\tsh ./scripts/verify-t058.sh\n\ndeliver-t058:\n\tsh ./scripts/deliver-t058.sh\n' >> "$tmp/Makefile"
fi

cd "$tmp"
go test -run '^TestQuorumPolicy' ./hat/hatReplication
go test ./hat/hatReplication
go test -race ./hat/hatReplication
go test -run '^TestQuorumPolicyRejectsZeroValueAndInvalidAcknowledgements$$' ./hat/hatReplication

git -C "$tmp" add \
	INSPIRATION.md \
	README.md \
	Makefile \
	QUORUM_POLICY.md \
	hat/hatReplication/quorum_policy_read.go \
	hat/hatReplication/quorum_policy_read_test.go \
	hat/hatReplication/quorum_policy_read_edge_test.go \
	scripts/format-t058.sh \
	scripts/test-t058-stage.sh \
	scripts/test-t058-edge.sh \
	scripts/verify-t058.sh \
	scripts/deliver-t058.sh
git -C "$tmp" commit -m "feat(replication): add reusable quorum policy evaluation"
git -C "$tmp" push origin HEAD:master
