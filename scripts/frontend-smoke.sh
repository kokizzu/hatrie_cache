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

run_pm_exec() {
	if [ "$pm" = "pnpm" ]; then
		pnpm exec "$@"
	else
		bunx "$@"
	fi
}

bool_true() {
	case "$1" in
		1|true|TRUE|yes|YES|on|ON)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

find_browser() {
	if [ -n "${CHROME_BIN:-}" ] && command -v "$CHROME_BIN" >/dev/null 2>&1; then
		printf '%s\n' "$CHROME_BIN"
		return 0
	fi
	for candidate in google-chrome chromium chromium-browser; do
		if command -v "$candidate" >/dev/null 2>&1; then
			command -v "$candidate"
			return 0
		fi
	done
	return 1
}

port=${FRONTEND_SMOKE_PORT:-0}
if [ "$port" = "0" ] || [ -z "$port" ]; then
	port=$(node -e 'const net=require("net"); const s=net.createServer(); s.listen(0, "127.0.0.1", () => { console.log(s.address().port); s.close(); });')
fi
base_url="http://127.0.0.1:$port"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-frontend-smoke.XXXXXX")
server_log="$tmp_dir/vite-preview.log"
server_pid=

cleanup() {
	if [ -n "$server_pid" ]; then
		kill "$server_pid" >/dev/null 2>&1 || true
		wait "$server_pid" >/dev/null 2>&1 || true
	fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

cd "$FRONTEND"
if ! bool_true "${FRONTEND_SMOKE_SKIP_BUILD:-false}"; then
	run_pm run build
fi

run_pm_exec vite preview --host 127.0.0.1 --port "$port" --strictPort >"$server_log" 2>&1 &
server_pid=$!

node - "$base_url" <<'NODE'
const baseURL = process.argv[2];
const routes = [
  { path: '/', title: 'HATrie Cache Dashboard', tokens: ['<div id="app"></div>', '/assets/'] },
  { path: '/keys.html', title: 'HATrie Cache Keys', tokens: ['<div id="app"></div>', '/assets/'] },
  { path: '/commands.html', title: 'HATrie Cache Commands', tokens: ['<div id="app"></div>', '/assets/'] },
  { path: '/admin.html', title: 'HATrie Cache Admin', tokens: ['<div id="app"></div>', '/assets/'] }
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchWithRetry(path) {
  const url = `${baseURL}${path}`;
  let lastError;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.text();
      }
      lastError = new Error(`${url} returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }
  throw lastError;
}

for (const route of routes) {
  const html = await fetchWithRetry(route.path);
  if (!html.includes(`<title>${route.title}</title>`)) {
    throw new Error(`${route.path} missing title ${route.title}`);
  }
  for (const token of route.tokens) {
    if (!html.includes(token)) {
      throw new Error(`${route.path} missing token ${token}`);
    }
  }
}
NODE

browser=$(find_browser || true)
if [ -n "$browser" ]; then
	dom_file="$tmp_dir/admin.dom"
	browser_log="$tmp_dir/browser.log"
	if ! "$browser" --headless --disable-gpu --no-sandbox --disable-dev-shm-usage --virtual-time-budget=3000 --dump-dom "$base_url/admin.html" >"$dom_file" 2>"$browser_log"; then
		cat "$server_log" >&2
		cat "$browser_log" >&2
		exit 1
	fi
	grep -q 'LevelDB Storage' "$dom_file"
	grep -q 'Replication' "$dom_file"
	grep -q 'Audit Trail' "$dom_file"
else
	if bool_true "${FRONTEND_SMOKE_REQUIRE_BROWSER:-false}"; then
		echo "Chrome/Chromium not found for browser smoke" >&2
		exit 1
	fi
	echo "frontend smoke: browser render skipped; Chrome/Chromium not found"
fi

echo "frontend smoke: ok $base_url"
