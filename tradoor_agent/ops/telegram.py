"""Telegram ops channel: status, pause/resume, params, kill switch, alerts.

Owner-only. If TELEGRAM_BOT_TOKEN or TELEGRAM_OWNER_IDS is empty the
component idles (agent still runs; you just have no remote control).
"""
from __future__ import annotations

import asyncio
import json
import time

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, filters

from ..bus import TOPIC_ALERT
from ..component import Component

_KILL_CONFIRM_WINDOW_SEC = 30
_LEVEL_ICON = {"info": "ℹ️", "warn": "⚠️", "error": "🔴", "trade": "💹"}


def _fmt_uptime(sec: float) -> str:
    sec = int(sec)
    d, rem = divmod(sec, 86_400)
    h, rem = divmod(rem, 3_600)
    m, _ = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


class TelegramOps(Component):
    name = "telegram_ops"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self._app: Application | None = None
        self._pending_kill_at: float = 0.0
        self._last_alert: tuple[str, float] = ("", 0.0)

    # ------------------------------------------------------------ lifecycle

    async def run(self) -> None:
        s = self.ctx.settings
        if not s.telegram_bot_token or not s.owner_ids:
            self.log.warning("telegram ops disabled (no token or no owner ids)")
            while not await self.sleep(5):
                self.beat()
            return

        self._app = Application.builder().token(s.telegram_bot_token).build()
        owner = filters.User(user_id=sorted(s.owner_ids))
        cmds = {
            "start": self.cmd_status,
            "status": self.cmd_status,
            "ping": self.cmd_ping,
            "pause": self.cmd_pause,
            "resume": self.cmd_resume,
            "watch": self.cmd_watch,
            "positions": self.cmd_positions,
            "trades": self.cmd_trades,
            "params": self.cmd_params,
            "set": self.cmd_set,
            "kill": self.cmd_kill,
            "help": self.cmd_help,
        }
        for name, cb in cmds.items():
            self._app.add_handler(CommandHandler(name, cb, filters=owner))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message"])
        self.log.info("telegram ops online for owners %s", sorted(s.owner_ids))

        alert_task = asyncio.create_task(self._alert_loop(), name="telegram_alerts")
        try:
            await asyncio.sleep(1)   # let the dashboard publish its URL first
            up = f"🟢 TheTradoor Agent up · mode={s.mode.value}"
            if self.ctx.web_url:
                up += f"\ndashboard: {self.ctx.web_url}"
            await self.send_all(up)
            while not await self.sleep(5):
                self.beat()
        finally:
            alert_task.cancel()
            try:
                await self.send_all("⚫ TheTradoor Agent shutting down" + (
                    f" (KILL: {self.ctx.state.kill_reason})" if self.ctx.state.kill_requested else ""
                ))
            except Exception:
                pass
            try:
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
            except Exception:
                self.log.exception("telegram shutdown error")

    # ------------------------------------------------------------ alerts

    def _targets(self) -> list[int | str]:
        s = self.ctx.settings
        if s.telegram_alert_chat_id:
            return [s.telegram_alert_chat_id]
        return sorted(s.owner_ids)

    async def send_all(self, text: str) -> None:
        if self._app is None:
            return
        for chat_id in self._targets():
            try:
                await self._app.bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:  # never let an alert kill the loop
                self.log.warning("send_message to %s failed: %s", chat_id, e)

    async def _alert_loop(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_ALERT, maxsize=500)
        while True:
            evt = await q.get()
            text, level = evt.get("text", ""), evt.get("level", "info")
            # de-dupe identical alerts inside 60s
            last_text, last_ts = self._last_alert
            if text == last_text and time.time() - last_ts < 60:
                continue
            self._last_alert = (text, time.time())
            await self.send_all(f"{_LEVEL_ICON.get(level, 'ℹ️')} {text}")

    # ------------------------------------------------------------ commands

    async def _status_text(self) -> str:
        st, s, db = self.ctx.state, self.ctx.settings, self.ctx.db
        open_n = await db.open_positions_count()
        pnl = await db.realized_pnl_today()
        snap = st.snapshot()
        hb = ", ".join(f"{k} {v}s" for k, v in sorted(snap["heartbeats_age_sec"].items())) or "-"
        counters = ", ".join(f"{k}={v}" for k, v in sorted(snap["counters"].items())) or "-"
        flag = "⏸ paused" if st.paused else "▶️ running"
        c = st.counters
        lines = [
            f"TheTradoor Agent · {flag}",
            f"mode: {s.mode.value} | llm lane: {'on' if s.llm_lane_enabled else 'off'}",
            f"uptime: {_fmt_uptime(st.uptime_sec)}",
            f"open positions: {open_n}/{s.max_open_positions}",
            f"pnl today: {pnl:+.4f} SOL (cap -{s.daily_loss_cap_sol})",
            f"ws: {c.get('ws.tokens_new', 0)} launches · {c.get('ws.migrations', 0)} migrations · {c.get('ws.trades', 0)} trades · {c.get('ws.connects', 0)} connects",
            f"screen: watching {c.get('screen.watchlist', 0)} · cheap-drop {c.get('screen.cheap_drop', 0)} · pass {c.get('screen.pass', 0)} · fail {c.get('screen.fail', 0)}",
        ]
        g = getattr(self.ctx, "gmgn", None)
        if g is not None:
            gs = g.stats
            cd = max(0, int(gs.cooldown_until - time.time()))
            lines.append(f"gmgn: {gs.requests} req · {gs.cache_hits} cached · {gs.errors} err · {gs.rate_limited} 429" + (f" · COOLDOWN {cd}s" if cd else ""))
        lines.append(f"heartbeats: {hb}")
        if self.ctx.web_url:
            lines.append(f"dashboard: {self.ctx.web_url}")
        return "\n".join(lines)

    async def cmd_watch(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        """Top of the watchlist: newest passes first."""
        rows = await self.ctx.db.fetchall(
            "SELECT mint, lane, reason, token_age_sec, breakdown_json, ts FROM candidates "
            "WHERE ts > ? ORDER BY ts DESC LIMIT 60", (time.time() - 3600,)
        )
        if not rows:
            await update.message.reply_text("belum ada kandidat dalam 1 jam terakhir.")
            return
        seen: set[str] = set()
        out: list[str] = []
        for r in rows:
            if r["mint"] in seen:
                continue
            seen.add(r["mint"])
            try:
                b = json.loads(r["breakdown_json"] or "{}")
            except json.JSONDecodeError:
                b = {}
            sym = b.get("symbol") or r["mint"][:6]
            mcap = b.get("mcap_usd")
            mcap_s = f"${mcap / 1000:.0f}k" if isinstance(mcap, (int, float)) else "n/a"
            mark = "✅" if r["lane"] == "screened" else "✖"
            out.append(f"{mark} {sym} · {int((r['token_age_sec'] or 0) // 60)}m · {mcap_s} · {r['reason'][:40]}")
            if len(out) >= 15:
                break
        await update.message.reply_text("\n".join(out))

    async def cmd_status(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(await self._status_text())

    async def cmd_ping(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text("pong")

    async def cmd_pause(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        self.ctx.state.paused = True
        self.log.warning("paused by owner %s", update.effective_user.id)
        await update.message.reply_text("⏸ paused: gak buka posisi baru. Posisi yang ada tetap di-manage. /resume buat lanjut.")

    async def cmd_resume(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        self.ctx.state.paused = False
        self.log.warning("resumed by owner %s", update.effective_user.id)
        await update.message.reply_text("▶️ resumed")

    async def cmd_positions(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        pos = await self.ctx.db.open_positions()
        if not pos:
            await update.message.reply_text("tidak ada posisi terbuka.")
            return
        now = time.time()
        lines = []
        for p in pos:
            m = p.get("meta") or {}
            ch = m.get("last_change")
            lines.append(f"${m.get('symbol') or p['mint'][:6]} · {p['lane']} · {p['size_sol']:.3f} SOL · {('%+.1f%%' % (ch * 100)) if ch is not None else '–'} · {int((now - p['opened_at']) // 60)}m")
        await update.message.reply_text("\n".join(lines))

    async def cmd_trades(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        rows = await self.ctx.db.fetchall("SELECT t.mint, t.pnl_sol, t.pnl_pct, t.exit_reason, t.holding_sec, p.meta_json FROM trades t LEFT JOIN positions p ON p.id=t.position_id ORDER BY t.closed_at DESC LIMIT 10")
        st = await self.ctx.db.trade_stats()
        if not rows:
            await update.message.reply_text("belum ada trade.")
            return
        lines = [f"total {st['trades']} · {st['wins']}W/{st['losses']}L · pnl {st['pnl_sol']:+.4f} SOL · streak {st['streak']:+d}"]
        for r in rows:
            try:
                sym = json.loads(r["meta_json"] or "{}").get("symbol") or r["mint"][:6]
            except json.JSONDecodeError:
                sym = r["mint"][:6]
            lines.append(f"${sym} · {r['exit_reason']} · {r['pnl_sol']:+.4f} SOL ({r['pnl_pct']:+.1f}%) · {int((r['holding_sec'] or 0) // 60)}m")
        await update.message.reply_text("\n".join(lines))

    async def cmd_params(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        params = await self.ctx.db.all_params()
        if not params:
            await update.message.reply_text("belum ada param override. /set KEY VALUE buat nambah.")
            return
        lines = [f"{k} = {json.dumps(v)}" for k, v in params.items()]
        await update.message.reply_text("\n".join(lines))

    async def cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text("format: /set KEY VALUE   (VALUE boleh angka, true/false, atau teks)")
            return
        key, raw = args[0], " ".join(args[1:])
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        await self.ctx.db.set_param(key, value, source=f"telegram:{update.effective_user.id}")
        self.log.warning("param %s set to %r by %s", key, value, update.effective_user.id)
        await update.message.reply_text(f"ok: {key} = {json.dumps(value)}")

    async def cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        args = context.args or []
        now = time.time()
        if args and args[0].lower() == "confirm":
            if now - self._pending_kill_at > _KILL_CONFIRM_WINDOW_SEC:
                await update.message.reply_text("gak ada /kill yang pending. Ketik /kill dulu.")
                return
            reason = f"telegram:{update.effective_user.id}"
            self.ctx.settings.kill_file.parent.mkdir(parents=True, exist_ok=True)
            self.ctx.settings.kill_file.write_text(f"{reason} {time.ctime()}\n")
            self.ctx.state.request_kill(reason)
            self.log.critical("KILL confirmed by %s", update.effective_user.id)
            await update.message.reply_text(
                "🛑 KILL. Agent berhenti dan gak bakal auto-start lagi.\n"
                f"Buat nyalain: hapus file {self.ctx.settings.kill_file} lalu restart service."
            )
            return
        self._pending_kill_at = now
        await update.message.reply_text(
            f"Yakin? Ketik /kill confirm dalam {_KILL_CONFIRM_WINDOW_SEC} detik.\n"
            "Kalau cuma mau stop entry baru, pakai /pause."
        )

    async def cmd_help(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "/status  kondisi agent\n"
            "/pause   stop entry baru (posisi tetap di-manage)\n"
            "/resume  lanjut entry\n"
            "/watch   kandidat 1 jam terakhir (✅ lolos hard gates)\n"
            "/positions posisi terbuka · /trades 10 trade terakhir\n"
            "/params  lihat override param\n"
            "/set K V override param live, misal /set gate_max_top10 0.25\n"
            "/kill    matiin agent (butuh /kill confirm)\n"
            "/ping"
        )
