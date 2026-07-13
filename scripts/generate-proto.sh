#!/usr/bin/env sh
set -eu

protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=module=hatrie_cache \
	--go-grpc_out=. \
	--go-grpc_opt=module=hatrie_cache \
	proto/hatriecache/v1/cache.proto
