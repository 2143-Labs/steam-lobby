# Stage indices below are load-bearing: buildah 5.8.x on the build machines
# cannot forward-reference a named stage in `COPY --from=` ("no stage or image
# found with that name") AND truncates a stage to just its FROM when a stage
# is defined before it. The verified-working layout is: frontend first
# (index 0), builder second (index 1) referencing it via `--from=0`, scratch
# last. Do not reorder or rename stages without re-verifying a full build.
FROM docker.io/library/node:24-alpine AS frontend
# The vite build resolves the shared game modules via ../../../ (web/), so
# the whole web/ tree must be present at /app/web — not just web/app/.
WORKDIR /app/web/app
COPY web/app/package.json web/app/package-lock.json ./
RUN npm ci
COPY web/ /app/web/
RUN npm run build   # tsc -b && vite build -> dist/index.html (single file)

FROM rust:1.97.1-alpine AS builder
# protoc (protobuf) + well-known-type proto includes (protobuf-dev):
# temporalio-prost / prost-wkt-types compile proto files at build time —
# mirrors shell.nix. ca-certificates + app user move here from the old runtime.
# Keep openssl-dev/pkgconfig even though nothing links openssl: removing them
# risks breaking a transitive build dep and the layer is cached anyway.
RUN apk add --no-cache musl-dev pkgconfig openssl-dev protobuf protobuf-dev ca-certificates \
 && addgroup -S app && adduser -S -G app app
WORKDIR /app

# Layer 1 — bake the dependency graph. Only manifests + stub sources are copied,
# so this layer is keyed on Cargo.toml/Cargo.lock and survives any source change.
# The real build below recompiles only the lobby-* crates (no build.rs in the
# workspace, so fingerprints match and external deps are reused).
# If you ADD a workspace member: add its Cargo.toml COPY + a stub source line here.
# NOTE: lobby-macros is a proc-macro crate and cannot export plain items, so its
# stub must be EMPTY (rustc rejects `pub fn` without #[proc_macro*]).
COPY Cargo.toml Cargo.lock ./
COPY lobby-core/Cargo.toml    lobby-core/
COPY lobby-macros/Cargo.toml  lobby-macros/
COPY lobby-client/Cargo.toml  lobby-client/
COPY lobby-server/Cargo.toml  lobby-server/
RUN mkdir -p lobby-core/src lobby-macros/src lobby-client/src lobby-server/src \
 && printf 'pub fn stub() {}\n' > lobby-core/src/lib.rs \
 && printf 'pub fn stub() {}\n' > lobby-client/src/lib.rs \
 && printf '' > lobby-macros/src/lib.rs \
 && printf 'pub fn stub() {}\n' > lobby-server/src/lib.rs \
 && printf 'fn main() {}\n'    > lobby-server/src/main.rs \
 && cargo build --release -p lobby-server

# Layer 2 — real sources. Cargo's freshness check is mtime-based: `COPY . .`
# preserves the repo's (old) mtimes, so the baked artifacts would look newer
# than the real sources and cargo would skip recompiling — shipping the stub
# binary. Touching the sources forces the lobby-* crates to rebuild, while
# external deps (in /usr/local/cargo, untouched) stay cached.
# `--from=0` = the frontend stage above (numeric: named forward refs fail on
# buildah 5.8.x). dist/ is gitignored, so `COPY . .` never brings it.
COPY . .
COPY --from=0 /app/web/app/dist/index.html /app/web/app/dist/index.html
RUN find lobby-core lobby-macros lobby-client lobby-server -name '*.rs' -exec touch {} + \
 && cargo build --release -p lobby-server

FROM scratch
COPY --from=1 /app/target/release/lobby-server /usr/local/bin/lobby-server
COPY --from=1 /app/lobby-server/migrations /migrations
# rustls bundles webpki-roots, so the binary works without these — kept as
# belt-and-braces for any future rustls-native-certs use.
COPY --from=1 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# scratch has no users; the app user from the builder lets USER app resolve.
COPY --from=1 /etc/passwd /etc/passwd
COPY --from=1 /etc/group /etc/group
USER app
EXPOSE 8080
# No HEALTHCHECK: scratch has no shell; the k8s Deployment probes /health already.
CMD ["/usr/local/bin/lobby-server"]
