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

port=${FRONTEND_BACKEND_SMOKE_PORT:-0}
if [ "$port" = "0" ] || [ -z "$port" ]; then
	port=$(node -e 'const net=require("net"); const s=net.createServer(); s.listen(0, "127.0.0.1", () => { console.log(s.address().port); s.close(); });')
fi
addr="127.0.0.1:$port"
base_url="http://$addr"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-frontend-backend-smoke.XXXXXX")
server_bin="$tmp_dir/hatrie-cache"
server_log="$tmp_dir/server.log"
server_pid=

cleanup() {
	if [ -n "$server_pid" ]; then
		kill "$server_pid" >/dev/null 2>&1 || true
		wait "$server_pid" >/dev/null 2>&1 || true
	fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

if ! bool_true "${FRONTEND_BACKEND_SMOKE_SKIP_BUILD:-false}"; then
	cd "$FRONTEND"
	run_pm run build
	cd "$ROOT"
fi

go build -o "$server_bin" ./cmd/hatrie-cache

"$server_bin" \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$FRONTEND/dist" \
	-node-id smoke-node \
	-db-path "$tmp_dir/cache.leveldb" \
	-audit-log-path "$tmp_dir/audit.jsonl" \
	-replication=true \
	-replication-async=true \
	-replication-queue-size 4 >"$server_log" 2>&1 &
server_pid=$!

node - "$base_url" <<'NODE'
const baseURL = process.argv[2];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function request(path, init) {
  const url = `${baseURL}${path}`;
  let lastError;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      const response = await fetch(url, init);
      if (response.ok) {
        const contentType = response.headers.get('content-type') || '';
        if (contentType.includes('application/json')) {
          return await response.json();
        }
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

const health = await request('/api/health');
if (health.status !== 'online' || health.node !== 'smoke-node') {
  throw new Error(`unexpected health ${JSON.stringify(health)}`);
}
await request('/api/commands', {
  method: 'POST',
  headers: { accept: 'application/json', 'content-type': 'application/json' },
  body: JSON.stringify({ command: 'SETSTR', key: 'smoke:admin', value: 'value' })
});
const storage = await request('/api/storage');
if (!storage.leveldb_configured || !String(storage.path || '').includes('cache.leveldb')) {
  throw new Error(`unexpected storage ${JSON.stringify(storage)}`);
}
const flush = await request('/api/storage/flush', {
  method: 'POST',
  headers: { accept: 'application/json', 'content-type': 'application/json' },
  body: '{}'
});
if (flush.store !== 'leveldb') {
  throw new Error(`unexpected flush ${JSON.stringify(flush)}`);
}
const replication = await request('/api/replication');
if (!replication.health || typeof replication.health_score !== 'number') {
  throw new Error(`unexpected replication ${JSON.stringify(replication)}`);
}
const audit = await request('/api/audit?limit=10');
if (!audit.configured || !audit.events.some((event) => event.action === 'storage.flush')) {
  throw new Error(`unexpected audit ${JSON.stringify(audit)}`);
}
const admin = await request('/admin.html');
if (!admin.includes('<title>HATrie Cache Admin</title>')) {
  throw new Error('admin HTML title missing');
}
NODE

browser=$(find_browser || true)
if [ -z "$browser" ]; then
	if bool_true "${FRONTEND_BACKEND_SMOKE_REQUIRE_BROWSER:-true}"; then
		echo "Chrome/Chromium not found for backend Admin smoke" >&2
		exit 1
	fi
	echo "frontend backend smoke: browser render skipped; Chrome/Chromium not found"
	echo "frontend backend smoke: ok $base_url"
	exit 0
fi

dom_file="$tmp_dir/admin.dom"
browser_log="$tmp_dir/browser.log"
if ! "$browser" --headless --disable-gpu --no-sandbox --disable-dev-shm-usage --virtual-time-budget=5000 --dump-dom "$base_url/admin.html" >"$dom_file" 2>"$browser_log"; then
	cat "$server_log" >&2
	cat "$browser_log" >&2
	exit 1
fi
grep -q 'LevelDB Storage' "$dom_file"
grep -q 'cache.leveldb' "$dom_file"
grep -q 'Audit Trail' "$dom_file"
grep -q 'storage.flush' "$dom_file"
grep -q 'Replication' "$dom_file"

echo "frontend backend smoke: ok $base_url"
