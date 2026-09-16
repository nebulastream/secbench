#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ./scripts/build-benchmark-images.sh [--skip-pull]

Builds the four local images used by emulator/config/experiment.yaml.
Set SECBENCH_WORKER_BASE_IMAGE or SECBENCH_CLI_BASE_IMAGE to override a base.
EOF
}

fail() {
    echo "build-benchmark-images: $*" >&2
    exit 1
}

pull=true
case "${1:-}" in
    "") ;;
    --skip-pull) pull=false ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
esac

command -v docker >/dev/null 2>&1 || fail "Docker is not installed"
docker info >/dev/null 2>&1 || fail "Docker is unavailable; configure current-user access or run this script with sudo"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
worker_base="${SECBENCH_WORKER_BASE_IMAGE:-nebulastream/worker:tt-tcp-sink}"
cli_base="${SECBENCH_CLI_BASE_IMAGE:-nebulastream/nes-cli:tt-tcp-sink}"

[[ -f "$repo_root/result-tracker/rtracker.cpp" ]] || fail \
    "result-tracker/rtracker.cpp is missing; the latency tracker must be present in the repository"

if [[ "$pull" == true ]]; then
    docker pull "$worker_base"
    docker pull "$cli_base"
fi

docker build --progress=plain \
    -f "$repo_root/emulator/docker/nes-tools.Dockerfile" \
    --build-arg "BASE_IMAGE=$worker_base" \
    -t secbench/nes-worker:local "$repo_root/emulator/docker"
docker build --progress=plain \
    -f "$repo_root/emulator/docker/nes-tools.Dockerfile" \
    --build-arg "BASE_IMAGE=$cli_base" \
    -t secbench/nes-cli:local "$repo_root/emulator/docker"
docker build --progress=plain \
    -f "$repo_root/datagen/docker/Dockerfile" \
    -t secbench/datagen:local "$repo_root/datagen"
docker build --progress=plain \
    -f "$repo_root/emulator/docker/tracker.Dockerfile" \
    -t secbench/tracker:local "$repo_root"

docker image inspect \
    secbench/nes-worker:local \
    secbench/nes-cli:local \
    secbench/datagen:local \
    secbench/tracker:local >/dev/null

echo "Benchmark images are ready."
