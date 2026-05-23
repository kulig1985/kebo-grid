#!/usr/bin/env bash
# Grid Finder venv setup — Python 3.8+ támogatva.
# Használat: ./setup.sh

set -e
cd "$(dirname "$0")"

# Python detect: preferált 3.11 > 3.10 > 3.9 > 3 (fallback)
for PY in python3.12 python3.11 python3.10 python3.9 python3 python; do
    if command -v "$PY" >/dev/null 2>&1; then
        PYTHON_BIN="$PY"
        break
    fi
done

if [ -z "${PYTHON_BIN:-}" ]; then
    echo "[!] Nincs python telepítve. Telepítsd: apt install python3 python3-venv python3-pip" >&2
    exit 1
fi

PY_VER=$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "[+] Használt Python: $PYTHON_BIN ($PY_VER)"

if [ ! -d ".venv" ]; then
    echo "[+] venv létrehozás (.venv)..."
    "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "[+] pip upgrade..."
pip install --upgrade pip >/dev/null

echo "[+] requirements telepítés..."
pip install -r requirements.txt

echo ""
echo "[OK] Telepítés kész. Futtatás:"
echo "     ./run.sh config.yaml          # fetch + report"
echo "     ./run.sh config.yaml --from-csv   # offline újra-szim"
