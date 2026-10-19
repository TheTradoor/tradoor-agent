#!/usr/bin/env bash
# One-shot install / update on the VPS. Safe to re-run.
#   bash deploy/install.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/tradoor-agent}"
SERVICE="tradoor-agent"

if [[ "$(pwd)" != "$APP_DIR" ]]; then
  echo "jalankan dari $APP_DIR (sekarang: $(pwd))"; exit 1
fi

PY="$(command -v python3 || command -v python3.12 || command -v python3.11 || command -v python3.10 || true)"
if [[ -z "$PY" ]]; then echo "python3 tidak ditemukan"; exit 1; fi
VER="$("$PY" -c 'import sys;print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
MINOR="${VER#3.}"
if [[ "${VER%%.*}" != "3" || "$MINOR" -lt 10 ]]; then echo "butuh Python 3.10+, dapat $VER"; exit 1; fi
if ! "$PY" -c "import ensurepip, venv" 2>/dev/null; then echo "modul venv belum ada: apt install -y ${PY##*/}-venv"; exit 1; fi

if [[ ! -d .venv ]]; then
  "$PY" -m venv .venv
fi
.venv/bin/pip install --upgrade pip wheel >/dev/null
.venv/bin/pip install -r requirements.txt

mkdir -p data
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo ">> .env dibuat dari .env.example. ISI DULU lalu jalankan lagi script ini."
  exit 0
fi

echo ">> config check"
.venv/bin/python -m tradoor_agent --check

echo ">> smoke test"
.venv/bin/python -m tradoor_agent --smoke

install -m 644 deploy/$SERVICE.service /etc/systemd/system/$SERVICE.service
systemctl daemon-reload
systemctl enable "$SERVICE" >/dev/null
systemctl restart "$SERVICE"
sleep 2
systemctl --no-pager --lines=15 status "$SERVICE" || true
echo
echo ">> log: journalctl -u $SERVICE -f"
PORT="$(grep -E '^WEB_PORT=' .env | cut -d= -f2)"; PORT="${PORT:-8787}"
if command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
  if ! ufw status | grep -qE "^$PORT/tcp"; then
    echo ">> ufw aktif tapi port $PORT belum dibuka. Jalankan: ufw allow $PORT/tcp"
  fi
fi
echo ">> dashboard: journalctl -u $SERVICE --no-pager | grep -m1 dashboard   (atau lihat pesan Telegram saat start)"
