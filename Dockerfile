FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev pkgconfig openssl-dev
WORKDIR /app
COPY . .
RUN cargo build --release -p lobby-server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/lobby-server /usr/local/bin/
COPY --from=builder /app/lobby-server/migrations /migrations
EXPOSE 8080
CMD ["lobby-server"]
