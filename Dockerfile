# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.22
ARG NODE_VERSION=22
ARG DEBIAN_VERSION=bookworm
ARG VCS_REF=unknown
ARG VERSION=dev
ARG BUILD_DATE=unknown

FROM node:${NODE_VERSION}-${DEBIAN_VERSION}-slim AS frontend
WORKDIR /src/svelte-mpa
COPY svelte-mpa/package.json svelte-mpa/pnpm-lock.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
	corepack enable && \
	corepack prepare pnpm@8.10.2 --activate && \
	pnpm install --frozen-lockfile
COPY svelte-mpa/ ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
	corepack enable && \
	corepack prepare pnpm@8.10.2 --activate && \
	pnpm run build

FROM golang:${GO_VERSION}-${DEBIAN_VERSION} AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	go mod download
COPY . .
COPY --from=frontend /src/svelte-mpa/dist ./svelte-mpa/dist
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/hatrie-cache ./cmd/hatrie-cache && \
	CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/hatrie-cli ./cmd/hatrie-cli

FROM debian:${DEBIAN_VERSION}-slim AS runtime
ARG VCS_REF=unknown
ARG VERSION=dev
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="hatrie_cache" \
	org.opencontainers.image.description="Experimental distributed cache using HAT-trie indexes" \
	org.opencontainers.image.source="https://github.com/kokizzu/hatrie_cache" \
	org.opencontainers.image.revision="${VCS_REF}" \
	org.opencontainers.image.version="${VERSION}" \
	org.opencontainers.image.created="${BUILD_DATE}" \
	org.opencontainers.image.vendor="kokizzu"
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends ca-certificates; \
	rm -rf /var/lib/apt/lists/*; \
	groupadd --system --gid 10001 hatrie-cache; \
	useradd --system --uid 10001 --gid hatrie-cache --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin hatrie-cache; \
	mkdir -p /app/svelte-mpa/dist /var/lib/hatrie-cache /var/backups/hatrie-cache; \
	chown -R hatrie-cache:hatrie-cache /app /var/lib/hatrie-cache /var/backups/hatrie-cache

COPY --from=builder /out/hatrie-cache /usr/local/bin/hatrie-cache
COPY --from=builder /out/hatrie-cli /usr/local/bin/hatrie-cli
COPY --from=frontend --chown=hatrie-cache:hatrie-cache /src/svelte-mpa/dist /app/svelte-mpa/dist

USER hatrie-cache
WORKDIR /app
EXPOSE 8080 9090
VOLUME ["/var/lib/hatrie-cache", "/var/backups/hatrie-cache"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD HATRIE_CACHE_AUTH_TOKEN="${MONITORING_AUTH_TOKEN:-}" /usr/local/bin/hatrie-cli -addr "${HATRIE_HEALTHCHECK_ADDR:-http://127.0.0.1:8080}" -timeout "${HATRIE_HEALTHCHECK_TIMEOUT:-2s}" health >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/hatrie-cache"]
CMD ["-monitoring-server", "-monitoring-addr", "0.0.0.0:8080", "-monitoring-web-dir", "/app/svelte-mpa/dist", "-db-path", "/var/lib/hatrie-cache/cache.leveldb", "-snapshot-path", "/var/lib/hatrie-cache/snapshot.hc", "-journal-path", "/var/lib/hatrie-cache/commands.journal"]
