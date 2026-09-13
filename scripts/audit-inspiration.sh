#!/usr/bin/env bash
set -eu

echo "== Recent commits =="
git log -5 --oneline
echo "== Inspiration rows =="
rg -c '^\| CH-' INSPIRATION_BACKLOG.md
rg -c '^\| MZ-' INSPIRATION_BACKLOG.md
rg -c '^\| TR-' INSPIRATION_BACKLOG.md
echo "== Unimplemented rows =="
rg -n '\[ \]' INSPIRATION_BACKLOG.md
echo "== Implemented rows =="
rg -n '\[x\]' INSPIRATION_BACKLOG.md
