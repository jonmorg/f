"""
Risk management: daily limits, consecutive-loss circuit breaker, stop-loss checks.
"""

import logging
from typing import Optional

log = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self._stop_reason = ""

    # ── Gate check (call before every trade) ─────────────────────────────────

    def can_trade(self) -> bool:
        """Return False if any daily risk limit has been breached."""
        stats = self.db.get_today_stats()

        if stats["realized_pnl"] <= -self.config.DAILY_LOSS_LIMIT:
            self._stop_reason = (
                f"Daily loss limit reached "
                f"(realised P&L ${stats['realized_pnl']:.2f} ≤ "
                f"-${self.config.DAILY_LOSS_LIMIT:.2f})"
            )
            log.warning(self._stop_reason)
            return False

        if stats["consecutive_losses"] >= self.config.MAX_CONSECUTIVE_LOSSES:
            self._stop_reason = (
                f"{stats['consecutive_losses']} consecutive losses — "
                f"limit is {self.config.MAX_CONSECUTIVE_LOSSES}"
            )
            log.warning(self._stop_reason)
            return False

        self._stop_reason = ""
        return True

    def stop_reason(self) -> str:
        return self._stop_reason

    # ── Position sizing ───────────────────────────────────────────────────────

    def size_position(self, raw_size: float) -> float:
        """
        Clamp a raw Kelly size to all hard limits.
        The final size is also the maximum we will lose if the market resolves
        against us (no external stop-loss needed when size ≤ MAX_LOSS_PER_TRADE).
        """
        size = min(raw_size, self.config.MAX_POSITION_ABS)
        size = min(size, self.config.STARTING_BALANCE * self.config.MAX_POSITION_PCT)
        # Never put more at risk than the per-trade loss limit
        size = min(size, self.config.MAX_LOSS_PER_TRADE)
        return round(max(0.0, size), 2)

    def compute_stop_price(self, entry_price: float, size_usdc: float) -> float:
        """
        Return the token price at which we should exit to keep loss ≤ MAX_LOSS_PER_TRADE.

        At entry_price P with size_usdc N, we hold N/P shares.
        If price falls to S, position value = (N/P) × S = N × (S/P).
        Loss = N − N×(S/P) = N×(1 − S/P).
        Solving for S such that loss = MAX_LOSS_PER_TRADE:
            S = P × (1 − MAX_LOSS_PER_TRADE / N)

        If N ≤ MAX_LOSS_PER_TRADE, we can afford to lose the entire stake,
        so we return 0 (no stop needed).
        """
        if size_usdc <= self.config.MAX_LOSS_PER_TRADE:
            return 0.0  # entire stake is within loss budget

        stop_frac = 1.0 - self.config.MAX_LOSS_PER_TRADE / size_usdc
        stop_price = entry_price * stop_frac
        return round(max(0.01, stop_price), 4)

    # ── Open-position stop-loss scanner ──────────────────────────────────────

    async def get_positions_to_stop(self, fetcher) -> list:
        """
        Inspect every open position.  Returns a list of PositionRow objects
        whose current mid-price has fallen to or below their stop_price.
        """
        positions = self.db.get_open_positions()
        to_stop = []
        for pos in positions:
            if pos.stop_price <= 0:
                continue  # full-loss position; no stop needed
            current = await fetcher.get_midprice(pos.token_id)
            if current is None:
                continue
            if current <= pos.stop_price:
                log.warning(
                    "STOP-LOSS triggered: %s | entry=%.4f stop=%.4f current=%.4f",
                    pos.question[:50], pos.entry_price, pos.stop_price, current,
                )
                to_stop.append(pos)
        return to_stop
