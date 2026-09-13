#!/bin/sh
set -eu

git branch --show-current
git branch --all --contains HEAD
git remote
git symbolic-ref --short refs/remotes/origin/HEAD
