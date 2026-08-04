# list all recipes
default:
  @just --list --unsorted

# build all crates
build:
  nix-shell shell.nix --run "cargo build --workspace"

# run all tests
test:
  nix-shell shell.nix --run "cargo test --workspace"

# run tests with output
test-verbose:
  nix-shell shell.nix --run "cargo test --workspace -- --nocapture"

# run DB-backed integration tests (needs `just db-up` first)
itest:
  nix-shell shell.nix --run "cargo test -p lobby-server --test integration -- --nocapture"

# lint with clippy
lint:
  nix-shell shell.nix --run "cargo clippy --all-targets -- -D warnings"

# lint + auto-fix
lint-fix:
  nix-shell shell.nix --run "cargo clippy --fix --allow-dirty --allow-staged"

# format with rustfmt
fmt:
  nix-shell shell.nix --run "cargo fmt --all"

# check formatting
fmt-check:
  nix-shell shell.nix --run "cargo fmt --all -- --check"

# run server (needs PostgreSQL + .env file)
run:
  nix-shell shell.nix --run "cargo run -p lobby-server"

# start database container (uses `docker compose` when available,
# otherwise falls back to a plain `docker run` — both produce the same
# postgres://lobby:lobby@localhost:5432/lobby that .env.example expects)
db-up:
  #!/usr/bin/env bash
  set -e
  if docker compose version &>/dev/null; then
    docker compose up -d db
  elif docker ps -q -f name=lobby-db | grep -q .; then
    echo "lobby-db already running"
  else
    docker run -d --name lobby-db \
      -e POSTGRES_USER=lobby -e POSTGRES_PASSWORD=lobby \
      -e POSTGRES_DB=lobby -p 5432:5432 \
      -v lobby-pgdata:/var/lib/postgresql/data \
      postgres:16-alpine
  fi
  # Wait until Postgres accepts connections on :5432.
  for _ in $(seq 1 30); do
    (echo > /dev/tcp/localhost/5432) 2>/dev/null && break
    sleep 1
  done
  echo "DB ready at localhost:5432"

# stop the database container(s)
db-down:
  #!/usr/bin/env bash
  set -e
  if docker compose version &>/dev/null; then
    docker compose down
  elif docker ps -q -f name=lobby-db | grep -q .; then
    docker rm -f lobby-db
  else
    echo "no lobby-db container to stop"
  fi
  echo "DB stopped"

# full local stack (Docker Compose reads .env automatically)
up:
  docker compose up -d

# stop full local stack
down:
  docker compose down

# clean build artifacts
clean:
  cargo clean
