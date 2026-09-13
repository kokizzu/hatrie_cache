#!/usr/bin/env bash
set -euo pipefail

expected_parent='40751ad98895d7bc6aa381b7b237fbdeb661a234'
current_commit=$(git rev-parse HEAD)
current_parent=$(git rev-parse HEAD^)
if [[ "$current_parent" != "$expected_parent" ]]; then
	printf 'refusing C201 push: expected parent %s, found %s\n' "$expected_parent" "$current_parent" >&2
	exit 1
fi

remote_commit=$(git ls-remote origin refs/heads/master | awk 'NR == 1 { print $1 }')
if [[ "$remote_commit" != "$expected_parent" ]]; then
	printf 'refusing C201 push: expected remote master %s, found %s\n' "$expected_parent" "${remote_commit:-<missing>}" >&2
	exit 1
fi

git push origin HEAD:master

verified_commit=$(git ls-remote origin refs/heads/master | awk 'NR == 1 { print $1 }')
if [[ "$verified_commit" != "$current_commit" ]]; then
	printf 'C201 push verification failed: expected %s, found %s\n' "$current_commit" "${verified_commit:-<missing>}" >&2
	exit 1
fi

printf 'pushed and verified %s on origin/master\n' "$current_commit"
