# Native Shopping Sessions pipeline. Build context: the native/ folder.
#   docker buildx build --platform linux/amd64 --load -t shopping-sessions-native:latest native
# Not the submission image: the repository's top-level Dockerfile (the DuckDB oracle) stays the entry point.

# Rust 1.85 on Debian bookworm, pinned by digest.
FROM rust:1.85.0-slim-bookworm@sha256:1829c432be4a592f3021501334d3fcca24f238432b13306a4e62669dec538e52 AS build
WORKDIR /src
# Fetch the pinned crates first, so a source change does not download them again.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && echo "" > src/lib.rs \
 && for b in bench_parse parquet_probe shopping_sessions; do echo "fn main(){}" > src/bin/$b.rs; done \
 && cargo fetch --locked
COPY src ./src
RUN cargo build --release --locked --bins && mkdir /out \
 && find target/release -maxdepth 1 -type f -executable -exec cp {} /out/ \;

# Debian bookworm slim (glibc for the Rust binary), pinned by digest. No Python, no shell needed at run time.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /out/ /app/
USER 1000:1000
CMD ["/app/shopping_sessions"]
