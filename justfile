# list all recipes
default:
  @just --list --unsorted
# Use the pinned nix-shell toolchain when Nix is installed, plain cargo otherwise.
has_nix := `if command -v nix-shell >/dev/null 2>&1; then echo 1; fi`
nix := if has_nix == "1" { "nix-shell shell.nix --run " } else { "" }


# build all crates
build:
  {{nix}}"cargo build --workspace"

test:
  {{nix}}"cargo test --workspace"

test-verbose:
  {{nix}}"cargo test --workspace -- --nocapture"

itest:
  {{nix}}"cargo test -p lobby-server --test integration -- --nocapture"

# JS-side determinism gauntlet + the Rust<->JS differential (needs node)
js-test:
  {{nix}}"node web/test/golden.mjs && node web/test/stress.mjs && node web/test/replica.mjs && node web/test/diff.mjs && cargo test -p lobby-core --test determinism -- --ignored differential_js_matches_rust"

lint:
  {{nix}}"cargo clippy --all-targets -- -D warnings"

lint-fix:
  {{nix}}"cargo clippy --fix --allow-dirty --allow-staged"

fmt:
  {{nix}}"cargo fmt --all"

fmt-check:
  {{nix}}"cargo fmt --all -- --check"

run:
  {{nix}}"cargo run -p lobby-server"


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
  # Wait until Postgres accepts connections on :5432 (check inside the
  # container, so this works on any host where `docker` does).
  if docker compose version &>/dev/null; then
    for _ in $(seq 1 30); do
      docker compose exec -T db pg_isready -U lobby &>/dev/null && break
      sleep 1
    done
  else
    for _ in $(seq 1 30); do
      docker exec lobby-db pg_isready -U lobby &>/dev/null && break
      sleep 1
    done
  fi
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
