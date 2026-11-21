#!/bin/sh
# Build the native binary (and run its unit tests) in the pinned Rust image, into the Docker volume
# native-target (/target/release/shopping_sessions), the binary tests/native_check.py and bench_native.sh run.
#   sh tests/build_native.sh [test]
set -eu
export MSYS_NO_PATHCONV=1
SRC=$(cd "$(dirname "$0")/../native" && pwd -W)   # Windows form, for Docker Desktop
IMG=rust:1.85.0-slim-bookworm@sha256:1829c432be4a592f3021501334d3fcca24f238432b13306a4e62669dec538e52
CMD="cargo build --release --locked --bin shopping_sessions"
[ "${1:-}" = test ] && CMD="cargo test --release --locked --lib && $CMD"
docker run --rm --platform linux/amd64 -v "$SRC":/src:ro -v native-target:/target -v cargo-registry:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/target -w /src "$IMG" sh -c "$CMD"
