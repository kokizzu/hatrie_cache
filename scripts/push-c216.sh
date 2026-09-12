#!/usr/bin/env bash
set -euo pipefail

expected_base="${C216_EXPECTED_BASE:-885f580d26e06df8e7e9e0246718423a92ebacee}"
head="$(git rev-parse HEAD)"
parent="$(git rev-parse HEAD^)"
if [[ "$parent" != "$expected_base" ]]; then
	printf 'refusing C216 push: expected parent %s, got %s\n' "$expected_base" "$parent" >&2
	exit 1
fi

read -r remote_head _ < <(git ls-remote origin refs/heads/master)
if [[ "$remote_head" != "$expected_base" ]]; then
	printf 'refusing C216 push: origin/master changed to %s\n' "$remote_head" >&2
	exit 1
fi

git push origin HEAD:master
read -r remote_after _ < <(git ls-remote origin refs/heads/master)
if [[ "$remote_after" != "$head" ]]; then
	printf 'C216 push verification failed: origin/master is %s, expected %s\n' "$remote_after" "$head" >&2
	exit 1
fi
