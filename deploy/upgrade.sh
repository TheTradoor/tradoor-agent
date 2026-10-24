#!/usr/bin/env bash
# Update the agent on the VPS to the latest main. Safe to re-run.
#   cd /opt/tradoor-agent && bash deploy/upgrade.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/tradoor-agent}"
SERVICE="tradoor-agent"
cd "$APP_DIR"

echo ">> git pull"
git pull --ff-only

# v0.8: exits and lane filters have new defaults in config.py. The v0.7 .env.example wrote the old values into .env,
# and .env wins over defaults, so those lines are removed (once) to let the new defaults apply. Nothing else is touched.
STALE=(EXIT_TP_PCT EXIT_SL_PCT EXIT_TRAIL_ARM_PCT EXIT_TRAIL_PCT EXIT_MAX_HOLD_SEC
       DEXPAID_EXIT_TP_PCT DEXPAID_EXIT_SL_PCT DEXPAID_EXIT_TRAIL_ARM_PCT DEXPAID_EXIT_TRAIL_PCT DEXPAID_EXIT_MAX_HOLD_SEC
       DEXPAID_MIN_HOLDERS DEXPAID_MAX_TOKEN_AGE_SEC DEXPAID_SIZE_MULT DEXPAID_MAX_CHASE_1M)
for k in "${STALE[@]}"; do
  if grep -qE "^${k}=" .env; then
    echo ">> .env: hapus ${k} (pakai default v0.8)"
    sed -i "/^${k}=/d" .env
  fi
done

.venv/bin/pip install -q -r requirements.txt
echo ">> config check"
.venv/bin/python -m tradoor_agent --check
install -m 644 deploy/$SERVICE.service /etc/systemd/system/$SERVICE.service
systemctl daemon-reload
systemctl restart "$SERVICE"
sleep 3
systemctl --no-pager --lines=8 status "$SERVICE" || true
echo
echo ">> versi: $(.venv/bin/python -c 'import tradoor_agent;print(tradoor_agent.__version__)') · log: journalctl -u $SERVICE -f"
