#!/usr/bin/env bash
set -euo pipefail

baseline=/tmp/hatrie-cache-t-u33-auth-baseline
baseline_commit="${BASELINE_COMMIT:-ecebd6cc}"
git worktree remove --force "$baseline" >/dev/null 2>&1 || true
git worktree add --detach "$baseline" "$baseline_commit" >/dev/null
cleanup() {
	git worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf '%s\n' "--- before: $baseline_commit legacy authorization ---"
(cd "$baseline" && go test ./hat/hatAuth -run '^$' -bench '^BenchmarkPolicyAuthorizeLegacy$' -benchmem -count=5)

printf '%s\n' '--- after: current legacy and function authorization ---'
go test ./hat/hatAuth -run '^$' -bench '^BenchmarkPolicyAuthorize(Legacy|Function)$' -benchmem -count=5
