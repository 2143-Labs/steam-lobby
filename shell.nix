{ pkgs ? import (builtins.fetchGit {
    url = "https://github.com/NixOS/nixpkgs";
    rev = "643809054d65fdd466a63e3155b8c498cb483c04";
  }) {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    cargo
    rustc
    rustfmt
    clippy
    gcc
    pkg-config
    openssl
    just
    protobuf   # protoc — prost-wkt-types (temporalio SDK dep) needs it at build time
  ];
}
