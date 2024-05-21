#!/usr/bin/env bash

ROOT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")/.."

set -x -e -o pipefail

NAME=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].name')
VERSION=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].version')

function build {
  local target=$1
  local target_output_name=$2
  local output_name="$NAME-$VERSION-$target_output_name"
  cross build --target "$target" --release
  mkdir -p "$ROOT_DIR/scripts/target"
  cp "$ROOT_DIR/target/$target/release/$NAME" "$ROOT_DIR/scripts/target/$output_name"
  rm -f "$ROOT_DIR/scripts/target/$output_name.bz2"
  bzip2 -9 "$ROOT_DIR/scripts/target/$output_name"
}

build aarch64-apple-darwin darwin-arm64
build aarch64-unknown-linux-musl linux-arm64
build x86_64-unknown-linux-musl linux-amd64
