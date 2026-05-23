#!/usr/bin/env bash
# Grid Finder runner — automatikusan a .venv-ből futtat.
# Használat: ./run.sh config.yaml [--from-csv]

set -e
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "[!] Nincs .venv — futtasd először: ./setup.sh" >&2
    exit 1
fi

# shellcheck disable=SC1091
source .venv/bin/activate

exec python finder.py "$@"
