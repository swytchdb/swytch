#!/bin/sh
# install swytch
#
#   curl -fsSL https://raw.githubusercontent.com/swytchdb/swytch/main/scripts/install.sh | sh
#
# environment:
#   SWYTCH_VERSION   release tag to install (default: latest)
#   SWYTCH_INSTALL   install directory (default: $HOME/.local/bin, fallback /usr/local/bin)
#   SWYTCH_VERIFY    set to 0 to skip checksum verification (not recommended)

set -eu

REPO="swytchdb/swytch"
BIN="swytch"

err() { printf 'install: %s\n' "$*" >&2; exit 1; }
info() { printf 'install: %s\n' "$*"; }

need() { command -v "$1" >/dev/null 2>&1 || err "missing required command: $1"; }
need uname
need tar
need mktemp

if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1" -o "$2"; }
    fetch_stdout() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO "$2" "$1"; }
    fetch_stdout() { wget -qO- "$1"; }
else
    err "need curl or wget"
fi

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
    linux|darwin) ;;
    mingw*|msys*|cygwin*)
        err "for Windows, run install.ps1 in PowerShell:
  iwr -useb https://raw.githubusercontent.com/$REPO/main/scripts/install.ps1 | iex" ;;
    *) err "unsupported OS: $os (download manually from https://github.com/$REPO/releases)" ;;
esac

raw_arch=$(uname -m)
case "$raw_arch" in
    x86_64|amd64) arch="x86_64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) err "unsupported architecture: $raw_arch" ;;
esac

version="${SWYTCH_VERSION:-}"
if [ -z "$version" ]; then
    info "resolving latest release"
    version=$(fetch_stdout "https://api.github.com/repos/$REPO/releases/latest" \
        | grep -E '"tag_name":' \
        | head -n1 \
        | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    [ -n "$version" ] || err "could not determine latest version"
fi
case "$version" in
    v*) version_num=${version#v} ;;
    *) version_num=$version; version=v$version ;;
esac

archive="${BIN}_${version_num}_${os}_${arch}.tar.gz"
base="https://github.com/$REPO/releases/download/$version"
url="$base/$archive"
sums_url="$base/checksums.txt"

tmp=$(mktemp -d 2>/dev/null || mktemp -d -t swytch)
trap 'rm -rf "$tmp"' EXIT INT TERM

info "downloading $archive"
fetch "$url" "$tmp/$archive" || err "download failed: $url"

if [ "${SWYTCH_VERIFY:-1}" = "1" ]; then
    info "verifying checksum"
    fetch "$sums_url" "$tmp/checksums.txt" || err "checksum download failed: $sums_url"
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$tmp" && grep " $archive\$" checksums.txt | sha256sum -c -) >/dev/null \
            || err "checksum mismatch"
    elif command -v shasum >/dev/null 2>&1; then
        (cd "$tmp" && grep " $archive\$" checksums.txt | shasum -a 256 -c -) >/dev/null \
            || err "checksum mismatch"
    else
        err "no sha256sum or shasum found; set SWYTCH_VERIFY=0 to skip (not recommended)"
    fi
fi

info "extracting"
tar -xzf "$tmp/$archive" -C "$tmp"
[ -f "$tmp/$BIN" ] || err "expected binary $BIN not found in archive"
chmod +x "$tmp/$BIN"

dest_dir="${SWYTCH_INSTALL:-}"
if [ -z "$dest_dir" ]; then
    if [ -w "$HOME/.local/bin" ] || mkdir -p "$HOME/.local/bin" 2>/dev/null; then
        dest_dir="$HOME/.local/bin"
    else
        dest_dir="/usr/local/bin"
    fi
fi
mkdir -p "$dest_dir" 2>/dev/null || true

if [ -d "$dest_dir" ] && [ -w "$dest_dir" ]; then
    mv "$tmp/$BIN" "$dest_dir/$BIN"
else
    info "installing to $dest_dir requires sudo"
    command -v sudo >/dev/null 2>&1 || err "sudo not available; create $dest_dir with appropriate permissions or set SWYTCH_INSTALL to a writable directory"
    sudo mkdir -p "$dest_dir"
    sudo mv "$tmp/$BIN" "$dest_dir/$BIN"
fi

info "installed $BIN $version to $dest_dir/$BIN"

case ":$PATH:" in
    *":$dest_dir:"*) ;;
    *) info "warning: $dest_dir is not on PATH; add it to your shell rc" ;;
esac
