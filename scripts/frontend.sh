#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
FRONTEND="$ROOT/svelte-mpa"

. "$ROOT/scripts/node-env.sh"

pm=${PACKAGE_MANAGER:-pnpm}
if ! command -v "$pm" >/dev/null 2>&1; then
	if command -v bun >/dev/null 2>&1; then
		pm=bun
	else
		echo "package manager not found; install pnpm or bun" >&2
		exit 1
	fi
fi

run_pm() {
	if [ "$pm" = "pnpm" ]; then
		pnpm "$@"
	else
		bun "$@"
	fi
}

require_frontend() {
	test -f "$FRONTEND/package.json"
	test -f "$FRONTEND/index.html"
	test -f "$FRONTEND/keys.html"
	test -f "$FRONTEND/commands.html"
	test -f "$FRONTEND/admin.html"
	test -f "$FRONTEND/src/pages/Dashboard.svelte"
	test -f "$FRONTEND/src/pages/Keys.svelte"
	test -f "$FRONTEND/src/pages/Commands.svelte"
	test -f "$FRONTEND/src/pages/Admin.svelte"
}

static_verify() {
	require_frontend
	node -e "JSON.parse(require('fs').readFileSync('$FRONTEND/package.json', 'utf8'))"
	grep -q 'src/dashboard.ts' "$FRONTEND/index.html"
	grep -q 'src/keys.ts' "$FRONTEND/keys.html"
	grep -q 'src/commands.ts' "$FRONTEND/commands.html"
	grep -q 'src/admin.ts' "$FRONTEND/admin.html"
	grep -q 'loadStats' "$FRONTEND/src/pages/Dashboard.svelte"
	grep -q 'loadEntries' "$FRONTEND/src/pages/Keys.svelte"
	grep -q 'runCommand' "$FRONTEND/src/pages/Commands.svelte"
	grep -q 'loadStorageStatus' "$FRONTEND/src/pages/Admin.svelte"
	grep -q 'compactStorage' "$FRONTEND/src/pages/Admin.svelte"
}

cmd=${1:-verify}

case "$cmd" in
	install)
		cd "$FRONTEND"
		run_pm install
		;;
	dev|check|test|build|preview)
		cd "$FRONTEND"
		run_pm run "$cmd"
		;;
	verify)
		static_verify
		if [ -d "$FRONTEND/node_modules" ]; then
			cd "$FRONTEND"
			run_pm run check
			run_pm run test
			run_pm run build
		else
			echo "frontend dependencies are not installed; package checks skipped"
			echo "run 'make frontend-install' before full frontend verification"
		fi
		;;
	*)
		echo "usage: $0 {install|dev|check|test|build|preview|verify}" >&2
		exit 2
		;;
esac
