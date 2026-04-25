"""
Telegram notifications via the Bot HTTP API.
All sends are best-effort — failures are logged but never crash the bot.
"""

import logging
from typing import Optional

import httpx

log = logging.getLogger(__name__)

_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = httpx.Timeout(10.0)


class TelegramAlerter:
    def __init__(self, config):
        self.token = config.TELEGRAM_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID
        self._enabled = bool(self.token and self.chat_id)
        self._http = httpx.AsyncClient(timeout=_TIMEOUT)

        if not self._enabled:
            log.warning(
                "Telegram not configured — alerts disabled. "
                "Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env"
            )

    async def close(self):
        await self._http.aclose()

    # ── Generic message ───────────────────────────────────────────────────────

    async def send(self, text: str):
        if not self._enabled:
            log.info("TG (disabled): %s", text[:120])
            return
        try:
            await self._http.post(
                _SEND_URL.format(token=self.token),
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
        except Exception as exc:
            log.warning("Telegram send failed: %s", exc)

    # ── Signal alert ──────────────────────────────────────────────────────────

    async def send_signal(self, market: dict, signal) -> None:
        q = market.get("question", "?")[:80]
        msg = (
            f"<b>SIGNAL</b>  {signal.side}\n"
            f"<i>{q}</i>\n"
            f"Price: {signal.price:.4f}  |  Est prob: {signal.probability:.1%}\n"
            f"Edge: <b>{signal.edge:.1%}</b>  |  Size: <b>${signal.size:.2f}</b>"
        )
        await self.send(msg)

    # ── Trade placed alert ────────────────────────────────────────────────────

    async def send_trade(self, market: dict, signal, order_result, stop_price: float) -> None:
        q = market.get("question", "?")[:80]
        stop_str = f"${stop_price:.4f}" if stop_price > 0 else "none (full loss = within budget)"
        msg = (
            f"<b>TRADE PLACED</b>  {signal.side}\n"
            f"<i>{q}</i>\n"
            f"Order: <code>{order_result.order_id}</code>\n"
            f"Price: {signal.price:.4f}  |  Size: ${signal.size:.2f}\n"
            f"Stop-loss at: {stop_str}\n"
            f"Max loss: ${min(signal.size, 1.0):.2f}"
        )
        await self.send(msg)

    # ── Stop-loss triggered alert ─────────────────────────────────────────────

    async def send_stop_loss(self, pos, order_result) -> None:
        status = "SENT" if order_result.success else f"FAILED: {order_result.error}"
        msg = (
            f"<b>STOP-LOSS</b> {pos.side}\n"
            f"<i>{pos.question[:70]}</i>\n"
            f"Entry: {pos.entry_price:.4f}  |  Stop: {pos.stop_price:.4f}\n"
            f"Sell order: {status}"
        )
        await self.send(msg)

    # ── Daily summary ─────────────────────────────────────────────────────────

    async def send_daily_summary(self, stats: dict, balance: float) -> None:
        pnl = stats["realized_pnl"]
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        msg = (
            f"<b>Daily Summary</b>\n"
            f"Trades: {stats['trades']}  |  Wins: {stats['wins']}\n"
            f"Realised P&L: {pnl_str}\n"
            f"Consecutive losses: {stats['consecutive_losses']}\n"
            f"Balance: ~${balance:.2f}"
        )
        await self.send(msg)

    # ── Risk limit hit alert ──────────────────────────────────────────────────

    async def send_risk_halt(self, reason: str) -> None:
        msg = f"<b>TRADING HALTED</b>\n{reason}"
        await self.send(msg)
