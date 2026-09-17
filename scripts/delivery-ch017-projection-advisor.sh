#!/bin/sh
set -eu
git status --short
git log -1 --oneline
