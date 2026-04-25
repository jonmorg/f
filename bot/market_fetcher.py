"""
Fetches markets from Polymarket's Gamma REST API and price data from the CLOB API.
"""

import json
import logging
from typing import Optional

import httpx

log = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(15.0, connect=5.0)


class MarketFetcher:
    def __init__(self, config):
        self.config = config
        self._http = httpx.AsyncClient(timeout=_TIMEOUT)

    async def close(self):
        await self._http.aclose()

    # ── Market list ───────────────────────────────────────────────────────────

    async def get_active_markets(self) -> list[dict]:
        """
        Return high-liquidity, active binary markets ordered by 24 h volume.
        Each dict is normalised by _enrich().
        """
        try:
            resp = await self._http.get(
                f"{self.config.GAMMA_API}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "archived": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                    "limit": self.config.MARKET_LIMIT,
                },
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as exc:
            log.error("Gamma API error: %s", exc)
            return []

        # Gamma returns a plain list or {"data": [...]}
        markets = raw if isinstance(raw, list) else raw.get("data", [])

        results = []
        for m in markets:
            if self._qualifies(m):
                enriched = self._enrich(m)
                if enriched:
                    results.append(enriched)

        log.debug("Fetched %d qualifying markets", len(results))
        return results

    def _qualifies(self, m: dict) -> bool:
        vol = float(m.get("volume24hr") or m.get("volume") or 0)
        liq = float(m.get("liquidity") or 0)
        active = bool(m.get("active", False))
        closed = bool(m.get("closed", True))
        archived = bool(m.get("archived", True))
        return (
            active
            and not closed
            and not archived
            and vol >= self.config.MIN_VOLUME_24H
            and liq >= self.config.MIN_LIQUIDITY
        )

    def _enrich(self, m: dict) -> Optional[dict]:
        """Normalise raw Gamma market into a flat dict the bot consumes."""
        # Gamma sometimes stores tokens as a JSON string
        tokens = m.get("tokens") or []
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                tokens = []

        # Fall back to outcomePrices / outcomes parallel arrays
        if not tokens:
            outcomes_raw = m.get("outcomes") or "[]"
            prices_raw = m.get("outcomePrices") or "[]"
            if isinstance(outcomes_raw, str):
                outcomes_raw = json.loads(outcomes_raw)
            if isinstance(prices_raw, str):
                prices_raw = json.loads(prices_raw)
            tokens = [
                {"outcome": o, "price": float(p)}
                for o, p in zip(outcomes_raw, prices_raw)
            ]

        yes_tok = next(
            (t for t in tokens if str(t.get("outcome", "")).upper() == "YES"), None
        )
        no_tok = next(
            (t for t in tokens if str(t.get("outcome", "")).upper() == "NO"), None
        )

        if not yes_tok or not no_tok:
            return None

        yes_price = float(yes_tok.get("price", 0))
        no_price = float(no_tok.get("price", 0))

        # Skip near-resolved markets (too little value to bet on)
        if yes_price <= 0.03 or yes_price >= 0.97:
            return None

        return {
            "id": m.get("conditionId") or m.get("id", ""),
            "condition_id": m.get("conditionId", ""),
            "question": m.get("question", ""),
            "slug": m.get("slug", ""),
            "end_date": m.get("endDate", ""),
            "volume_24h": float(m.get("volume24hr") or m.get("volume") or 0),
            "liquidity": float(m.get("liquidity") or 0),
            "yes_token_id": yes_tok.get("token_id", ""),
            "no_token_id": no_tok.get("token_id", ""),
            "yes_price": yes_price,
            "no_price": no_price,
        }

    # ── Price history ─────────────────────────────────────────────────────────

    async def get_price_history(
        self, token_id: str, interval: str = "1w", fidelity: int = 60
    ) -> list[dict]:
        """
        Hourly candles for the past week.
        Returns list of {"t": unix_ts, "p": float} dicts.
        """
        if not token_id:
            return []
        try:
            resp = await self._http.get(
                f"{self.config.CLOB_API}/prices-history",
                params={"market": token_id, "interval": interval, "fidelity": fidelity},
            )
            resp.raise_for_status()
            return resp.json().get("history", [])
        except Exception as exc:
            log.warning("Price history error for %s: %s", token_id[:12], exc)
            return []

    # ── Order book ────────────────────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> dict:
        """Snapshot of best bids/asks for a token."""
        if not token_id:
            return {}
        try:
            resp = await self._http.get(
                f"{self.config.CLOB_API}/book",
                params={"token_id": token_id},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Order book error for %s: %s", token_id[:12], exc)
            return {}

    # ── Current mid-price from CLOB ───────────────────────────────────────────

    async def get_midprice(self, token_id: str) -> Optional[float]:
        """Returns (best_bid + best_ask) / 2, or None on failure."""
        book = await self.get_order_book(token_id)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        try:
            bid = float(bids[0]["price"])
            ask = float(asks[0]["price"])
            return (bid + ask) / 2.0
        except (KeyError, ValueError, TypeError):
            return None
