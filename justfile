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

# start database container
db-up:
  docker compose up -d db
  @echo "Waiting for PostgreSQL..."
  sleep 3
  @echo "DB ready at localhost:5432"

# stop all containers
db-down:
  docker compose down

# full local stack (Docker Compose reads .env automatically)
up:
  docker compose up -d

# stop full local stack
down:
  docker compose down

# clean build artifacts
clean:
  cargo clean
