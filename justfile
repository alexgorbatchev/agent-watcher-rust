default:
    @just --list

build:
    cargo build --all-targets

test:
    cargo test --all-targets
    cargo test --doc

fmt:
    cargo fmt --package agent-watcher

lint:
    cargo fmt --package agent-watcher -- --check
    cargo clippy --all-targets -- -D warnings

coverage:
    cargo llvm-cov --all-targets --fail-under-lines 90

check: lint test coverage
