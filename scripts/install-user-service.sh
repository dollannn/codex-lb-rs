#!/usr/bin/env bash
set -euo pipefail

if (( EUID == 0 )); then
  printf 'error: run this installer as your regular user, not as root\n' >&2
  exit 1
fi

for command_name in cargo install mktemp systemctl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'error: required command not found: %s\n' "$command_name" >&2
    exit 1
  fi
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
if [[ -n ${CARGO_TARGET_DIR:-} ]]; then
  if [[ $CARGO_TARGET_DIR = /* ]]; then
    target_dir=$CARGO_TARGET_DIR
  else
    target_dir="$repo_root/$CARGO_TARGET_DIR"
  fi
else
  target_dir="$repo_root/target"
fi
binary_source="$target_dir/release/codex-lb-rs"
binary_dir="$HOME/.local/bin"
binary_target="$binary_dir/codex-lb-rs"
unit_source="$repo_root/packaging/systemd/codex-lb-rs.service"
unit_dir="${XDG_CONFIG_HOME:-"$HOME/.config"}/systemd/user"
unit_target="$unit_dir/codex-lb-rs.service"

if [[ ! -f "$unit_source" ]]; then
  printf 'error: service unit not found: %s\n' "$unit_source" >&2
  exit 1
fi

install_atomically() {
  local source=$1
  local target=$2
  local mode=$3
  local temporary

  temporary=$(mktemp "${target}.XXXXXX")
  if ! install -m "$mode" -- "$source" "$temporary"; then
    rm -f -- "$temporary"
    return 1
  fi
  if ! mv -f -- "$temporary" "$target"; then
    rm -f -- "$temporary"
    return 1
  fi
}

printf 'Building codex-lb-rs in release mode...\n'
(
  cd -- "$repo_root"
  CARGO_TARGET_DIR="$target_dir" cargo build --release --locked
)

if [[ ! -x "$binary_source" ]]; then
  printf 'error: build did not produce %s\n' "$binary_source" >&2
  exit 1
fi

install -d -m 0755 -- "$binary_dir" "$unit_dir"
install_atomically "$binary_source" "$binary_target" 0755
install_atomically "$unit_source" "$unit_target" 0644

systemctl --user daemon-reload
systemctl --user enable codex-lb-rs.service
systemctl --user restart codex-lb-rs.service

printf 'Installed %s and started codex-lb-rs.service\n' "$binary_target"
