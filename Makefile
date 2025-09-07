.PHONY: clippy ci tests

ci: clippy tests

clippy:
	cargo clippy --manifest-path ./Cargo.toml -- -D warnings -D clippy::pedantic -A clippy::missing_errors_doc
	cargo clippy --no-default-features --manifest-path ./Cargo.toml -- -D warnings -D clippy::pedantic -A clippy::missing_errors_doc

tests:
	cargo test
