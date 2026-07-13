.PHONY: test verify verify-go verify-c verify-frontend bench frontend-install frontend-dev frontend-check frontend-test frontend-build

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
