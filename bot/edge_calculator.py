"""
Edge calculation and Half-Kelly position sizing.

Default probability model: EMA(24 h) mean-reversion + order-book imbalance.

To plug in an external source (sports odds, news NLP, Metaculus, etc.),
subclass EdgeCalculator and override `get_external_probability`.
"""

import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class TradeSignal:
    market_id: str
    side: str           # "YES" or "NO"
    token_id: str
    price: float        # price paid per share (0–1)
    size: float         # USDC to spend
    edge: float         # net edge after fees/slippage (e.g. 0.12 = 12 %)
    probability: float  # our estimated win probability


class EdgeCalculator:
    """
    Analyses a market and returns a TradeSignal when net edge > MIN_EDGE.
    Uses an EMA mean-reversion heuristic by default; override
    `get_external_probability` to inject a better probability source.
    """

    def __init__(self, config, fetcher):
        self.config = config
        self.fetcher = fetcher
        self._bankroll = config.STARTING_BALANCE

    def update_bankroll(self, amount: float):
        self._bankroll = max(amount, 1.0)

    # ── Public API ────────────────────────────────────────────────────────────

    async def analyze_market(self, market: dict) -> Optional[TradeSignal]:
        """
        Full analysis pipeline for one market.
        Returns a TradeSignal or None if no edge above threshold.
        """
        yes_price = market["yes_price"]
        no_price = market["no_price"]

        # Try caller-supplied external probability first
        ext_prob = await self.get_external_probability(market)

        if ext_prob is None:
            history = await self.fetcher.get_price_history(
                market["yes_token_id"], interval="1w", fidelity=60
            )
            book = await self.fetcher.get_order_book(market["yes_token_id"])
            ext_prob = self._estimate_probability(yes_price, history, book)

        if ext_prob is None:
            return None

        cost = self.config.total_cost()  # fee + slippage

        yes_edge = ext_prob - yes_price - cost
        no_edge = (1.0 - ext_prob) - no_price - cost

        # Select the more favourable side
        if yes_edge >= no_edge and yes_edge >= self.config.MIN_EDGE:
            side, price, prob, edge, token_id = (
                "YES", yes_price, ext_prob, yes_edge, market["yes_token_id"]
            )
        elif no_edge > yes_edge and no_edge >= self.config.MIN_EDGE:
            side, price, prob, edge, token_id = (
                "NO", no_price, 1.0 - ext_prob, no_edge, market["no_token_id"]
            )
        else:
            log.debug(
                "No edge: %s | yes_edge=%.3f no_edge=%.3f",
                market["question"][:50], yes_edge, no_edge,
            )
            return None

        size = self._kelly_size(edge, price)
        if size < 0.50:  # not worth placing below 50 ¢
            return None

        log.info(
            "SIGNAL  %-55s | %s @ %.3f  edge=%.1f%%  size=$%.2f",
            market["question"][:55], side, price, edge * 100, size,
        )
        return TradeSignal(
            market_id=market["id"],
            side=side,
            token_id=token_id,
            price=price,
            size=size,
            edge=edge,
            probability=prob,
        )

    # ── Default internal probability model ───────────────────────────────────

    def _estimate_probability(
        self,
        current_price: float,
        history: list[dict],
        order_book: dict,
    ) -> Optional[float]:
        """
        Blends two signals:
          1. EMA(24 h) mean-reversion  — assume 35 % reversion toward 24 h average
          2. Order-book bid/ask imbalance — heavy bids imply fair value > mid

        Returns estimated YES probability or None if both signals are weak.
        """
        ema_est = self._ema_signal(current_price, history)
        book_est = self._book_signal(current_price, order_book)

        estimates = [e for e in (ema_est, book_est) if e is not None]
        if not estimates:
            return None

        blended = sum(estimates) / len(estimates)

        # Discard if estimate barely differs from market price
        if abs(blended - current_price) < 0.05:
            return None

        return round(max(0.02, min(0.98, blended)), 4)

    def _ema_signal(
        self, current_price: float, history: list[dict]
    ) -> Optional[float]:
        """
        EMA mean-reversion: if current price is >MIN_DEVIATION below/above the
        24-candle EMA, estimate partial reversion toward the EMA.
        """
        prices = [float(p["p"]) for p in history if "p" in p]
        if len(prices) < max(12, self.config.EMA_PERIOD // 2):
            return None

        ema = self._ema(prices, min(len(prices), self.config.EMA_PERIOD))
        deviation = ema - current_price  # positive → price below EMA

        if abs(deviation) < self.config.MIN_DEVIATION:
            return None

        return current_price + deviation * self.config.REVERSION_FACTOR

    def _book_signal(
        self, current_price: float, order_book: dict
    ) -> Optional[float]:
        """
        Order-book imbalance: large bid depth relative to ask depth nudges the
        fair-value estimate slightly above mid-price, and vice-versa.
        Shift is capped at ±5 % of mid-price.
        """
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])
        if len(bids) < 3 or len(asks) < 3:
            return None

        try:
            bid_depth = sum(float(b["size"]) for b in bids[:10])
            ask_depth = sum(float(a["size"]) for a in asks[:10])
        except (KeyError, ValueError):
            return None

        total = bid_depth + ask_depth
        if total == 0:
            return None

        # bid_ratio > 0.5 → more buy pressure → nudge estimate upward
        bid_ratio = bid_depth / total
        max_shift = current_price * 0.05
        shift = (bid_ratio - 0.5) * max_shift * 2  # linear ±max_shift

        try:
            mid = (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
        except (KeyError, ValueError, IndexError):
            mid = current_price

        return mid + shift

    # ── Kelly sizing ──────────────────────────────────────────────────────────

    def _kelly_size(self, edge: float, price: float) -> float:
        """
        Half-Kelly stake in USDC.

        Kelly fraction for a binary bet:
            f* = edge / (1 - price)
        where edge = our_prob - market_price (after fees).

        Capped at MAX_POSITION_ABS and MAX_POSITION_PCT × bankroll.
        """
        denom = 1.0 - price
        if denom < 0.01:
            return 0.0

        kelly_f = edge / denom
        if self.config.HALF_KELLY:
            kelly_f /= 2.0

        size = kelly_f * self._bankroll
        size = min(size, self.config.MAX_POSITION_ABS)
        size = min(size, self._bankroll * self.config.MAX_POSITION_PCT)
        return round(max(0.0, size), 2)

    # ── Static helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _ema(prices: list[float], period: int) -> float:
        k = 2.0 / (period + 1)
        val = prices[0]
        for p in prices[1:]:
            val = p * k + val * (1 - k)
        return val

    # ── Extension hook ────────────────────────────────────────────────────────

    async def get_external_probability(self, market: dict) -> Optional[float]:
        """
        Override to inject an external probability estimate (0–1) for YES.
        Return None to fall back to the internal mean-reversion model.

        Example integrations:
          • The Odds API  — sports win probabilities from bookmaker odds
          • Metaculus / Manifold — crowd forecasts for political/news markets
          • OpenAI / Hugging Face NLP — sentiment/news-based signal
          • Your own statistical model
        """
        return None
