#!/usr/bin/env sh
set -eu

image=${DOCKER_IMAGE:-hatrie-cache:latest}
dockerfile=${DOCKERFILE:-Dockerfile}
context=${DOCKER_CONTEXT:-.}
platform=${DOCKER_PLATFORM:-}
target=${DOCKER_TARGET:-}

set -- -f "$dockerfile" -t "$image" "$@"
if [ -n "$platform" ]; then
	set -- --platform "$platform" "$@"
fi
if [ -n "$target" ]; then
	set -- --target "$target" "$@"
fi

exec docker build "$@" "$context"
