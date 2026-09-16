#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: sudo ./scripts/setup-emulator-host.sh
       ./scripts/setup-emulator-host.sh --check

Installs the Linux host tools required by SECBench's direct-routed Containernet
deployment. Docker must be installed separately.
EOF
}

fail() {
    echo "setup-emulator-host: $*" >&2
    exit 1
}

mode=install
case "${1:-}" in
    "") ;;
    --check) mode=check ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
esac

[[ "$(uname -s)" == Linux ]] || fail "Containernet requires a Linux host or Linux VM"
[[ -r /etc/os-release ]] || fail "cannot identify the distribution: /etc/os-release is missing"

# shellcheck disable=SC1091
. /etc/os-release
os_id="${ID,,}"
os_version="${VERSION_ID:-unknown}"
os_like=" ${ID_LIKE:-} "
ubuntu_base="${UBUNTU_CODENAME:-${VERSION_CODENAME:-unknown}}"

case "$os_id" in
    ubuntu)
        support="supported"
        ;;
    linuxmint)
        support="supported through Ubuntu base ${ubuntu_base}"
        ;;
    debian)
        support="best effort"
        ;;
    *)
        if [[ "$os_like" == *" ubuntu "* || "$os_like" == *" debian "* ]]; then
            support="best effort Debian-family compatibility"
        else
            fail "unsupported distribution '$os_id'; use Ubuntu 22.04/24.04, Linux Mint 22.x, or a Linux VM"
        fi
        ;;
esac

echo "Detected ${PRETTY_NAME:-$os_id $os_version} (${support})"
if [[ "$os_id" == linuxmint ]]; then
    [[ "$ubuntu_base" != unknown ]] || fail "Linux Mint does not expose UBUNTU_CODENAME in /etc/os-release"
    echo "Ubuntu package base: ${ubuntu_base} (do not use Mint codename ${VERSION_CODENAME:-unknown} for Docker's Ubuntu repository)"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
required_commands=(docker mnexec ip tc)

check_host() {
    local missing=()
    local executable
    for executable in "${required_commands[@]}"; do
        command -v "$executable" >/dev/null 2>&1 || missing+=("$executable")
    done
    if ((${#missing[@]})); then
        echo "Missing host commands: ${missing[*]}" >&2
        return 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo "Docker is installed but its daemon is unavailable to the current user" >&2
        return 1
    fi
    echo "Host checks passed: docker, mnexec, ip, and tc are available"
}

if [[ "$mode" == check ]]; then
    check_host
    exit
fi

((EUID == 0)) || fail "installation requires root; run: sudo ./scripts/setup-emulator-host.sh"
command -v apt-get >/dev/null 2>&1 || fail "this setup currently supports apt-based distributions only"
command -v docker >/dev/null 2>&1 || fail "install Docker first: https://docs.docker.com/engine/install/ubuntu/"
docker info >/dev/null 2>&1 || fail "Docker is installed but the daemon is not running"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
    build-essential ca-certificates cgroup-tools ethtool iproute2 iptables \
    net-tools psmisc python3-dev python3-pip python3-venv socat

build_dir="$(mktemp -d /tmp/secbench-mnexec.XXXXXX)"
trap 'rm -f "$build_dir/mnexec"; rmdir "$build_dir" 2>/dev/null || true' EXIT
cc -Wall -Wextra -DVERSION='"secbench"' "$repo_root/emulator/mnexec.c" -o "$build_dir/mnexec"
install -m 0755 "$build_dir/mnexec" /usr/bin/mnexec

check_host
echo "Host preparation complete. Run scripts/setup-emulator-python.sh as your normal user."
