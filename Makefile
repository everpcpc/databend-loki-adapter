.PHONY: fmt clippy lint test build run bump-major bump-minor bump-patch

CARGO ?= cargo

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

lint: fmt clippy

test:
	$(CARGO) test --all-features -- --nocapture

build:
	$(CARGO) build --all-features

run:
	$(CARGO) run --all-features -- $(ARGS)

bump-major:
	./scripts/bump_version.py major

bump-minor:
	./scripts/bump_version.py minor

bump-patch:
	./scripts/bump_version.py patch
