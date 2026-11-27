.PHONY: dev check test build

export CARGO_BUILD_JOBS ?= 2

dev:
	uv sync --locked

check: dev
	cargo-fmt --all --check
	cargo-clippy clippy --workspace --all-targets -- -D warnings
	uv run --no-sync ruff format --check python tests build_backend.py scripts
	uv run --no-sync ruff check python tests build_backend.py scripts
	uv run --no-sync ty check

test: dev
	cargo test --workspace --locked
	uv run --no-sync pytest -q

build:
	uv build --no-sources
