#!/usr/bin/env bash
# Build oati-ts-monitor .deb on Debian/Ubuntu (or via Docker on macOS).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

build_native() {
    command -v dpkg-buildpackage >/dev/null 2>&1 || {
        echo "dpkg-buildpackage not found. Install: apt install build-essential debhelper devscripts" >&2
        exit 1
    }
    chmod +x debian/rules packaging/usr/bin/*
    dpkg-buildpackage -us -uc -b
    echo ""
    echo "Built:"
    ls -1 ../*.deb 2>/dev/null || ls -1 ../oati-ts-monitor_*.deb
}

build_docker() {
    local image="${DEB_BUILD_IMAGE:-debian:bookworm-slim}"
    echo "Building inside $image ..."
    docker run --rm \
        -v "$ROOT:/src" \
        -v "$ROOT/..:/out" \
        -w /src \
        "$image" \
        bash -ec '
            set -euo pipefail
            apt-get update -qq
            DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
                build-essential debhelper devscripts dpkg-dev >/dev/null
            chmod +x debian/rules packaging/usr/bin/*
            dpkg-buildpackage -us -uc -b
            mv ../*.deb /out/ 2>/dev/null || mv ../oati-ts-monitor_*.deb /out/ 2>/dev/null || true
            ls -1 /out/*.deb
        '
}

if [[ "${1:-}" == "--docker" ]]; then
    shift
    build_docker "$@"
elif [[ "${1:-}" == "--native" ]]; then
    shift
    build_native "$@"
elif [[ "$(uname -s)" == "Darwin" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "Docker required on macOS. Start Docker Desktop or build on Linux." >&2
        exit 1
    fi
    build_docker "$@"
else
    build_native "$@"
fi
