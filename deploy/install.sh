#!/usr/bin/env bash
# Install registryd on a systemd Linux host (Debian/Ubuntu tested).
#
#   curl -fsSL https://raw.githubusercontent.com/OWNER/REPO/main/deploy/install.sh | sudo bash
#
# What it does:
#   1. installs the registryd binary to /usr/local/bin (downloads the
#      latest GitHub release for this machine's architecture, or builds
#      from source with --from-source when cargo is available),
#   2. creates the registryd system user and /var/lib/registryd,
#   3. writes /etc/registryd/config.toml with a freshly generated API
#      token (kept if one already exists),
#   4. installs + enables the systemd unit and starts the daemon.
#
# Environment overrides:
#   REGISTRYD_REPO   owner/repo to download from (default: autodetected
#                    from this script's URL is not possible — set it when
#                    piping from curl; defaults to commonsdb/commonsdb-iroh-service)
#   REGISTRYD_VERSION  release tag (default: latest)
set -euo pipefail

REPO="${REGISTRYD_REPO:-commonsdb/commonsdb-iroh-service}"
VERSION="${REGISTRYD_VERSION:-latest}"
FROM_SOURCE=0
[ "${1:-}" = "--from-source" ] && FROM_SOURCE=1

[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }
command -v systemctl >/dev/null || { echo "systemd is required" >&2; exit 1; }

arch=$(uname -m)
case "$arch" in
  x86_64) target="x86_64-unknown-linux-gnu" ;;
  aarch64|arm64) target="aarch64-unknown-linux-gnu" ;;
  *) echo "unsupported architecture: $arch (use --from-source)" >&2; exit 1 ;;
esac

install_binary() {
  if [ "$FROM_SOURCE" -eq 1 ]; then
    command -v cargo >/dev/null || { echo "cargo not found; install Rust first" >&2; exit 1; }
    echo "building from source..."
    workdir=$(mktemp -d)
    git clone --depth 1 "https://github.com/$REPO" "$workdir/src"
    (cd "$workdir/src" && cargo build --release -p registryd)
    install -m 0755 "$workdir/src/target/release/registryd" /usr/local/bin/registryd
    rm -rf "$workdir"
    return
  fi
  if [ "$VERSION" = "latest" ]; then
    url="https://github.com/$REPO/releases/latest/download/registryd-$target.tar.gz"
  else
    url="https://github.com/$REPO/releases/download/$VERSION/registryd-$target.tar.gz"
  fi
  echo "downloading $url"
  tmp=$(mktemp -d)
  curl -fsSL "$url" -o "$tmp/registryd.tar.gz"
  tar -xzf "$tmp/registryd.tar.gz" -C "$tmp"
  install -m 0755 "$tmp/registryd" /usr/local/bin/registryd
  rm -rf "$tmp"
}

install_binary
echo "installed $(/usr/local/bin/registryd --version)"

id -u registryd >/dev/null 2>&1 || useradd --system --home /var/lib/registryd --shell /usr/sbin/nologin registryd
mkdir -p /var/lib/registryd /etc/registryd
chown registryd:registryd /var/lib/registryd
chmod 750 /var/lib/registryd

if [ ! -f /etc/registryd/config.toml ]; then
  token=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
  cat > /etc/registryd/config.toml <<EOF
# registryd configuration — see docs/operator-guide.md for every knob.
bind_addr = "127.0.0.1:8080"
data_dir = "/var/lib/registryd"
api_tokens = ["$token"]
EOF
  chown root:registryd /etc/registryd/config.toml
  chmod 640 /etc/registryd/config.toml
  echo "wrote /etc/registryd/config.toml (generated API token: $token)"
else
  echo "keeping existing /etc/registryd/config.toml"
fi

unit_src="$(dirname "$0")/registryd.service"
if [ -f "$unit_src" ]; then
  cp "$unit_src" /etc/systemd/system/registryd.service
else
  curl -fsSL "https://raw.githubusercontent.com/$REPO/main/deploy/registryd.service" \
    -o /etc/systemd/system/registryd.service
fi
systemctl daemon-reload
systemctl enable --now registryd

echo
echo "registryd is starting. Next steps:"
echo "  systemctl status registryd            # confirm it is running"
echo "  curl -s localhost:8080/health         # queue depth, counts"
echo "  curl -s localhost:8080/ticket         # the read ticket for storectl"
echo "  docs/operator-guide.md                # TLS, backups, imports, verify"
