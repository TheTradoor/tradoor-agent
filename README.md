# TheTradoor Agent

Solana trading agent. Hot path deterministik (ingest → hard gates → scoring → risk gate → eksekusi),
LLM cuma di zona abu-abu dengan timeout ketat, dan slow loop buat autopsy + usulan param.

Status: **Fase 2** (paper trading). Ingest → gates → scoring → routing → risk gate → paper broker → position manager,
semuanya dengan harga nyata dari GMGN tapi tanpa transaksi on-chain. Grafik balance di dashboard = equity paper.

## Alur fase 2

```
candidates (lolos hard gates)
   -> scorer: 0-100 dari momentum (buy pressure, swap rate, price change) · growth (kecepatan holder/mcap vs cek sebelumnya)
             · quality (top10, bundler, sniper hold, dev, fresh wallet, smart money, serial launcher, entrapment)
             · context (tahap bonding, socials, jumlah holder, band mcap). Bobot SCORE_W_* bisa diubah live.
   -> router: >= SCORE_HIGH → FAST · [SCORE_LOW, SCORE_HIGH) & umur >= GREY_MIN_TOKEN_AGE_SEC → GREY · sisanya DROP
   -> GREY: LLM review (LLM_LANE_ENABLED, butuh ANTHROPIC_API_KEY). JSON ketat, timeout → neutral, boost maks LLM_MAX_BOOST,
            size dikali LLM_LANE_SIZE_MULT. Default OFF: grey = skip.
   -> risk gate: kill/pause · MAX_OPEN_POSITIONS · posisi ganda · REENTRY_COOLDOWN_SEC · DAILY_LOSS_CAP_SOL · COOLDOWN_AFTER_LOSS_SEC
   -> paper broker: fill = harga GMGN ± PAPER_SLIPPAGE_BPS, fee PAPER_FEE_BPS, konversi USD→SOL via harga WSOL GMGN
   -> entry filters: ENTRY_MAX_CHASE_1M/5M (jangan beli yang udah lari), ENTRY_MIN_HOLDERS, ENTRY_MAX_SELL_RATIO_1M (distribusi), ENTRY_MIN_SWAPS_1M
   -> position manager: harga posisi dari TICK PumpPortal (subscribeTokenTrade per posisi, butuh PUMPPORTAL_API_KEY + wallet 0.02 SOL),
      sl · tp · trail dievaluasi tiap tick; GMGN tiap POSITION_CHECK_SEC buat dev_sold · holders_drop · time, dan jadi fallback harga
      kalau tick mati > TICK_STALE_SEC. → SELL → trades + equity
```

Pelajaran dari 9 jam paper pertama (v0.5): SL -10% kena di -26..-56% karena harga GMGN di-cache 15 detik, dan semua entry
masuk pas token lagi lari (hold 15-50 detik). Makanya v0.6: tick pricing, chase guard, SCORE_HIGH 80, bobot quality > momentum.

Semua keputusan tersimpan: `decisions` (termasuk alasan skip), `orders`, `fills`, `positions` (meta: skor, komponen, peak), `trades`,
`llm_reviews`. Itu bahan autopsy fase 5.

## Alur fase 1

```
PumpPortal WS (create/migrate, gratis)
   -> cheap gates (dev buy SOL, dev % supply, nama)          [0 API cost]
   -> watchlist (cek pertama +20s, lalu tiap 30s, maks 30 menit)
   -> GMGN /v1/token/info + /v1/token/security (weight 2 per cek, cache 15s)
   -> hard gates (holders, liq*, mcap, top10, dev, bundler, rat, snipers, sniper_hold, entrapment, honeypot, renounced)  *liq hanya setelah migrasi
   -> candidates (lane=screened | drop, breakdown_json = semua metrik GMGN, gates_json = hasil per gate)
   -> TOPIC_CANDIDATE (dikonsumsi scoring di fase 2) + alert Telegram (maks 20/jam)
```

Token yang gagal gate struktural (bundler, entrapment, honeypot, renounced, dev, rat) dua kali berturut-turut
dibuang dari watchlist biar weight gak kebuang. Budget: 150 token x 2 weight / 30s = 10 weight/s dari 50.

Semua threshold `GATE_*`, `CHEAP_*`, `SCREEN_*` bisa dioverride live tanpa restart: `/set gate_max_top10 0.25`
(dibaca ulang tiap 10 detik). `/params` buat lihat, `/watch` buat lihat kandidat 1 jam terakhir.

Catatan mapping GMGN (diverifikasi dari respons asli, bukan dokumentasi): `token/security` cuma memuat renounced / honeypot /
tax / lock; `suspected_insider_hold_rate`, `sniper_count`, `rug_ratio`, `is_wash_trading` TIDAK ada. Penggantinya dari `info.stat`:
`top70_sniper_hold_rate` → sniper_hold, `top_entrapment_trader_percentage` → entrapment, `private_vault_hold_rate`,
`creator_created_count`; jumlah sniper dari `wallet_tags_stat.sniper_wallets`. Panel Diagnostics → "gmgn fields returned"
selalu menampilkan struktur terbaru.

## Struktur

```
tradoor_agent/
  __main__.py      entry: python -m tradoor_agent [--check|--smoke]
  config.py        semua setting dari env / .env
  supervisor.py    boot, restart komponen yang crash, watchdog, KILL file
  component.py     Context + base class Component
  state.py         state runtime (paused, kill, heartbeats, counters)
  bus.py           pub/sub in-process (topik: token.new, trade, candidate, decision, order, position, alert)
  db.py            SQLite (WAL) + schema semua fase
  models.py        dataclass yang dipakai semua stage
  housekeeper.py   prune events, daily stats
  params.py        override param live dari tabel params (/set)
  screener.py      watchlist + GMGN snapshot + hard gates -> candidates
  ingest/pumpportal.py  WS client (reconnect, backoff, parser create/buy/sell/migrate)
  ingest/gmgn.py        OpenAPI client (X-APIKEY, weight bucket, cache, cooldown saat 429) + normalisasi snapshot
  gates/rules.py        cheap_gates + hard_gates (pure functions)
  ops/telegram.py  remote control + alert
  ops/web.py + ops/static/dashboard.html   dashboard web (API JSON + halaman)
  scoring/ risk/ llm_review/ execution/ positions/ slow_loop/   (fase berikutnya)
tests/test_phase1.py    replay test tanpa network: python -m tests.test_phase1
tests/test_dashboard.py tes semua route dashboard: python -m tests.test_dashboard
tests/test_phase2.py     replay buy → tp / sl paper: python -m tests.test_phase2
deploy/
  install.sh       setup venv + systemd di VPS, aman dijalankan ulang
  tradoor-agent.service
```

## Setup di VPS (Ubuntu, Python 3.10+)

```bash
# 1. taruh folder ini di /opt/tradoor-agent (scp dari laptop)
cd /opt/tradoor-agent
bash deploy/install.sh        # bikin .venv, install deps, bikin .env dari contoh

# 2. isi .env (minimal TELEGRAM_BOT_TOKEN + TELEGRAM_OWNER_IDS biar bisa dikontrol)
nano .env

# 3. jalankan lagi: check -> smoke -> pasang systemd -> start
bash deploy/install.sh

# log
journalctl -u tradoor-agent -f
```

Manual tanpa systemd:

```bash
.venv/bin/python -m tradoor_agent --check          # validasi config
.venv/bin/python -m tradoor_agent --smoke          # boot 3 detik di temp dir
.venv/bin/python -m tradoor_agent --check-gmgn     # tes key GMGN + mapping field
.venv/bin/python -m tradoor_agent --probe-ws 20    # lihat 20 frame mentah PumpPortal
.venv/bin/python -m tradoor_agent --dump-gmgn MINT # payload mentah GMGN untuk satu mint
.venv/bin/python -m tradoor_agent                  # jalan
```

Catatan PumpPortal: satu koneksi saja per API key. Jangan jalankan `--probe-ws` barengan service yang lagi hidup;
stop service dulu (`systemctl stop tradoor-agent`) atau pakai key lain.

## Mode

| MODE      | Wallet | Tx dikirim | Catatan |
|-----------|--------|------------|---------|
| `dry_run` | tidak  | tidak      | default. PnL virtual pakai harga nyata |
| `confirm` | ya     | setelah approve di Telegram | buat transisi |
| `live`    | ya     | ya         | butuh `I_UNDERSTAND_LIVE_TRADING=yes` |

## Dashboard web

Agent nyalain web kecil di port `WEB_PORT` (default 8787). Buka `http://IP-VPS:8787/?token=TOKEN`.
Token: isi `WEB_TOKEN` di `.env`, atau biarkan kosong dan agent bikin sendiri sekali lalu kirim link-nya ke Telegram saat start
(atau lihat `journalctl -u tradoor-agent | grep dashboard`). Setelah sekali buka dengan `?token=`, cookie nyimpen 30 hari.

Tampilan "survival mode" (tema kertas, monospace): header ALIVE / win streak / uptime / checks / pid, tile balance · P&L ·
launches/screened · win rate dengan sparkline, grafik balance history (equity paper, marker tiap trade), activity log
(klik baris PASS/FAIL buat buka detail gate + metrik GMGN), watchlist ladder, dan 7 kartu agent (INGEST, GATES, GMGN, SCORER,
LLM, RISK, EXEC) yang nyala saat aktif. Di bawah: Candidates, Params (ubah threshold live), WS frames, Diagnostics.
Balance / P&L / win rate baru bergerak setelah fase 2 (paper trade); saldo awal virtual = `PAPER_START_SOL` (default 5).
Tombol pause/resume ada di header. Kill tetap cuma lewat Telegram atau file `data/KILL`.

Berbagi VPS dengan bot lain (misal The Watchers): aman. Unit systemd dibatasi `MemoryMax=1G`, `CPUQuota=70%`, `Nice=5`.
Yang harus beda: token bot Telegram (bikin bot baru), dan kalau GMGN key-nya sama, set `GMGN_BUDGET_FRACTION=0.7`
biar sisa weight-nya cukup buat bot satunya.

Firewall: kalau `ufw status` aktif, buka portnya: `ufw allow 8787/tcp`. Kalau mau lebih aman, set `WEB_HOST=127.0.0.1`
dan akses lewat SSH tunnel: `ssh -L 8787:127.0.0.1:8787 root@IP-VPS` lalu buka `http://127.0.0.1:8787`.

## Kontrol

Telegram (owner saja): `/status` `/watch` `/positions` `/trades` `/pause` `/resume` `/params` `/set K V` `/kill` (lalu `/kill confirm`) `/help`

Kill switch tanpa Telegram (misal bot Telegram-nya mati):

```bash
touch /opt/tradoor-agent/data/KILL      # watchdog lihat dalam <= 10 detik, agent berhenti
rm /opt/tradoor-agent/data/KILL && systemctl restart tradoor-agent   # nyalakan lagi
```

Selama file `data/KILL` ada, agent menolak start (exit code 3) dan systemd tidak me-restart.
`/pause` beda: agent tetap jalan dan posisi tetap di-manage, cuma gak buka entry baru.

## Data

- `data/agent.sqlite` : semua tabel (tokens, events, candidates, llm_reviews, decisions, orders, fills, positions, trades, params, daily_stats)
- `data/logs/agent.log` : rotasi harian, simpan 14 hari
- Hari untuk daily stats / loss cap mengikuti `TIMEZONE` (default Asia/Jakarta)

## Roadmap

0. Fondasi ✅
1. Ingest (PumpPortal WS) + GMGN enrichment + hard gates → candidates ✅
2. Scoring + routing + risk gate + llm_review → paper trade ✅
3. Eksekusi (PumpPortal local tx): simulate → sign → send → confirm → reconcile; mode confirm dulu
4. Position manager + emergency exit
5. Slow loop: autopsy, LLM-lane vs rule-only, usulan param lewat backtest gate
6. Hardening: watchdog lanjutan, backup, alert policy
