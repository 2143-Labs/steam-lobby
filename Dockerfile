FROM rust:1.97.1-alpine AS builder
# protoc (protobuf) + well-known-type proto includes (protobuf-dev):
# temporalio-protos / prost-wkt-types compile proto files at build time —
# mirrors shell.nix.
RUN apk add --no-cache musl-dev pkgconfig openssl-dev protobuf protobuf-dev
WORKDIR /app
COPY . .
RUN cargo build --release -p lobby-server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder --chown=app:app /app/target/release/lobby-server /usr/local/bin/
COPY --from=builder --chown=app:app /app/lobby-server/migrations /migrations
USER app
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s CMD wget -qO- http://127.0.0.1:8080/health || exit 1
CMD ["lobby-server"]
