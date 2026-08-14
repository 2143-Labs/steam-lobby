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
  {{nix}}"DATABASE_URL=postgres://lobby:lobby@localhost:5432/lobby cargo test --workspace -- --test-threads 4 && just db-sweep"

test-verbose:
  {{nix}}"DATABASE_URL=postgres://lobby:lobby@localhost:5432/lobby cargo test --workspace -- --test-threads 4 --nocapture && just db-sweep"

itest:
  {{nix}}"DATABASE_URL=postgres://lobby:lobby@localhost:5432/lobby cargo test -p lobby-server --test integration -- --test-threads 4 --nocapture && just db-sweep"

# Parallel integration suite via cargo-nextest (per-test DBs from sqlx::test).
# The binary is a cargo subcommand: `cargo nextest` finds cargo-nextest on PATH.
test-fast:
  {{nix}}"DATABASE_URL=postgres://lobby:lobby@localhost:5432/lobby cargo nextest run --workspace && just db-sweep"

# Drop leftover sqlx::test databases from previous runs (skips any still in use).
# Fed via stdin (docker exec -i): psql's -c mode cannot mix SQL with the
# \gexec meta-command, but stdin processes meta-commands normally.
db-sweep:
  printf '%s\n' "SELECT format('DROP DATABASE IF EXISTS %I', datname) FROM pg_database WHERE datname LIKE '_sqlx_test%' \gexec" | docker exec -i lobby-db psql -U lobby -d lobby -q

# JS-side determinism gauntlet + the Rust<->JS differential (needs node)
js-test:
  {{nix}}"node web/test/golden.mjs && node web/test/stress.mjs && node web/test/replica.mjs && node web/test/diff.mjs && node web/test/wrtc.mjs && cargo test -p lobby-core --test determinism -- --ignored differential_js_matches_rust"

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


# start database container (uses `docker compose` when available, otherwise
# podman (this machine: no docker binaries), otherwise a plain `docker run`
# — all three produce the same postgres://lobby:lobby@localhost:5432/lobby
# that .env.example expects)
db-up:
  #!/usr/bin/env bash
  set -e
  if docker compose version &>/dev/null; then
    docker compose up -d db
  elif podman ps -q -f name=lobby-db | grep -q .; then
    echo "lobby-db already running"
  elif docker ps -q -f name=lobby-db | grep -q .; then
    echo "lobby-db already running"
  elif command -v podman >/dev/null 2>&1; then
    podman run -d --name lobby-db \
      -e POSTGRES_USER=lobby -e POSTGRES_PASSWORD=lobby \
      -e POSTGRES_DB=lobby -p 5432:5432 \
      -v lobby-pgdata:/var/lib/postgresql/data \
      postgres:16-alpine
  else
    docker run -d --name lobby-db \
      -e POSTGRES_USER=lobby -e POSTGRES_PASSWORD=lobby \
      -e POSTGRES_DB=lobby -p 5432:5432 \
      -v lobby-pgdata:/var/lib/postgresql/data \
      postgres:16-alpine
  fi
  # Wait until Postgres accepts connections on :5432 (check inside the
  # container, so this works on any host where the container tool does).
  if docker compose version &>/dev/null; then
    for _ in $(seq 1 30); do
      docker compose exec -T db pg_isready -U lobby &>/dev/null && break
      sleep 1
    done
  elif command -v podman >/dev/null 2>&1; then
    for _ in $(seq 1 30); do
      podman exec lobby-db pg_isready -U lobby &>/dev/null && break
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
  elif podman ps -q -f name=lobby-db | grep -q .; then
    podman rm -f lobby-db
  elif docker ps -q -f name=lobby-db | grep -q .; then
    docker rm -f lobby-db
  else
    echo "no lobby-db container to stop"
  fi
  echo "DB stopped"

# start the local Temporal stack (podman play kube; the only container tool on
# this machine is podman — no docker/podman-compose binaries). auto-setup needs
# ~30-60s to create the schema + default namespace on first boot.
temporal-up:
  #!/usr/bin/env bash
  set -e
  if podman pod exists temporal-dev &>/dev/null; then
    echo "temporal-dev already running"
  else
    podman play kube deploy/temporal.yaml
  fi
  # Wait until the frontend accepts TCP on :7233 (poll with a bash /dev/tcp connect).
  for _ in $(seq 1 90); do
    if (exec 3<>/dev/tcp/localhost/7233) 2>/dev/null; then
      exec 3>&- 3<&-
      break
    fi
    sleep 1
  done
  echo "Temporal ready at localhost:7233, UI at http://localhost:8233"

# stop the local Temporal stack
temporal-down:
  podman play kube --down deploy/temporal.yaml

up:
  docker compose up -d

# stop full local stack
down:
  docker compose down

# clean build artifacts
clean:
  cargo clean
