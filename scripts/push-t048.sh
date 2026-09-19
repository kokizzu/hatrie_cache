#!/usr/bin/env bash
set -euo pipefail

branch=${PUSH_BRANCH:-master}
git push origin "HEAD:refs/heads/$branch"
