#!/usr/bin/env bash
# Fails when the Dockerfile's dependency-baking layer drifts from the cargo
# workspace. Adding a crate to `[workspace] members` without a matching
# COPY + stub makes `cargo build -p lobby-server` die with "failed to load
# manifest for workspace member" — the build job was red for seven weeks that
# way (2026-09-27 → 2026-11-15). Run from anywhere; also runs in CI before the
# image build so the failure costs seconds, not minutes.
set -euo pipefail
cd "$(dirname "$0")/.."

# Members may be one line or several: collect quoted names from `members = [` to `]`.
# The `stop` flag matters: resetting on `grab && /\]/` after rewriting $0 misses the
# closing bracket when the array is multi-line, and then `resolver = "2"` gets parsed
# as a member. Verified against both a single-line and a multi-line workspace.
members=$(awk '
  /^[[:space:]]*members[[:space:]]*=/ { grab = 1 }
  grab && /\]/ { stop = 1 }
  grab { while (match($0, /"[^"]+"/)) { print substr($0, RSTART+1, RLENGTH-2); $0 = substr($0, RSTART+RLENGTH) } }
  grab && stop { grab = 0; stop = 0 }
' Cargo.toml)

if [ -z "$members" ]; then
  echo "::error file=Cargo.toml::could not parse [workspace] members"
  exit 1
fi

fail=0
for m in $members; do
  grep -qE "^COPY[[:space:]]+$m/Cargo\.toml[[:space:]]+$m/?[[:space:]]*$" Dockerfile \
    || { echo "::error file=Dockerfile::no 'COPY $m/Cargo.toml $m/' line for workspace member $m"; fail=1; }
  grep -qE ">[[:space:]]*$m/src/(lib|main)\.rs" Dockerfile \
    || { echo "::error file=Dockerfile::no stub source for workspace member $m"; fail=1; }
done

# The reverse direction: a COPY for a directory that is no longer a member.
for d in $(grep -oE '^COPY[[:space:]]+[a-z0-9_-]+/Cargo\.toml' Dockerfile | awk '{print $2}' | cut -d/ -f1); do
  printf '%s\n' "$members" | grep -qx "$d" \
    || { echo "::error file=Dockerfile::copies $d/Cargo.toml but $d is not a workspace member"; fail=1; }
done

[ "$fail" -eq 0 ] && echo "dockerfile/workspace parity OK: $(printf '%s\n' "$members" | wc -l | tr -d ' ') members"
exit "$fail"
