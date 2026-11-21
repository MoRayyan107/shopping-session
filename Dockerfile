# Shopping Sessions: the native (Rust) pipeline. Build from the repository root:
#   docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
# The DuckDB pipeline (the correctness oracle, tag oracle-v1) is the fallback: Dockerfile.duckdb.
# The scorers are linux/amd64 and build this file natively, so no FROM line pins a platform.

# Rust 1.85 on Debian bookworm, pinned by digest.
FROM rust:1.85.0-slim-bookworm@sha256:1829c432be4a592f3021501334d3fcca24f238432b13306a4e62669dec538e52 AS build
WORKDIR /src
# Fetch the locked crates first, so a source change does not download them again.
COPY native/Cargo.toml native/Cargo.lock ./
RUN mkdir -p src/bin && echo "" > src/lib.rs \
 && for b in bench_parse parquet_probe shopping_sessions; do echo "fn main(){}" > src/bin/$b.rs; done \
 && cargo fetch --locked
COPY native/src ./src
RUN cargo build --release --locked --bin shopping_sessions

# Debian bookworm slim (glibc for the binary), pinned by digest. No Python at run time.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /src/target/release/shopping_sessions /app/shopping_sessions
# At run time: read-only root, uid 1000, no network, /data read-only, /output and /scratch writable.
# The binary reads DATA_DIR / OUTPUT_DIR / NCPU from the environment and logs only to stderr.
USER 1000:1000
CMD ["/app/shopping_sessions"]
