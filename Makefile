MONITORING_ADDR ?= 127.0.0.1:8080
MONITORING_WEB_DIR ?= svelte-mpa/dist

.PHONY: test verify verify-go verify-c verify-frontend bench run monitoring-server frontend-install frontend-dev frontend-check frontend-test frontend-build

test: verify-go

verify: verify-go verify-c verify-frontend

verify-go:
	./scripts/verify-go.sh

verify-c:
	./scripts/verify-c.sh

verify-frontend:
	./scripts/frontend.sh verify

bench:
	go test -run '^$$' -bench=RawBytes -benchmem

run:
	@CMD='$(CMD)' ./scripts/run.sh

monitoring-server:
	MONITORING_ADDR='$(MONITORING_ADDR)' MONITORING_WEB_DIR='$(MONITORING_WEB_DIR)' ./scripts/monitoring-server.sh

frontend-install:
	./scripts/frontend.sh install

frontend-dev:
	./scripts/frontend.sh dev

frontend-check:
	./scripts/frontend.sh check

frontend-test:
	./scripts/frontend.sh test

frontend-build:
	./scripts/frontend.sh build
