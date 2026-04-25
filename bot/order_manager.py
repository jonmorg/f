"""
Order placement via py-clob-client.

py-clob-client is synchronous, so every call is dispatched through
asyncio.to_thread() to avoid blocking the event loop.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.constants import POLYGON
    from py_clob_client.order_builder.constants import BUY, SELL

    _CLOB_OK = True
except ImportError:
    _CLOB_OK = False
    log.warning(
        "py-clob-client not installed — order placement disabled. "
        "Run: pip install py-clob-client"
    )


@dataclass
class OrderResult:
    success: bool
    order_id: Optional[str] = None
    error: Optional[str] = None


class OrderManager:
    def __init__(self, config):
        self.config = config
        self._client: Optional["ClobClient"] = None
        self._wallet: Optional[str] = None

        if _CLOB_OK and config.PRIVATE_KEY:
            self._init_client()
        elif not config.PRIVATE_KEY:
            log.error("PRIVATE_KEY is empty — cannot place orders.")

    # ── Initialisation ────────────────────────────────────────────────────────

    def _init_client(self):
        try:
            from eth_account import Account

            acct = Account.from_key(self.config.PRIVATE_KEY)
            self._wallet = acct.address

            # Build an unauthenticated client first to derive/create API creds
            base = ClobClient(
                host=self.config.CLOB_API,
                key=self.config.PRIVATE_KEY,
                chain_id=POLYGON,
                signature_type=0,   # EOA — no proxy or gnosis-safe required
            )

            # derive_api_key() is deterministic from the private key
            try:
                creds = base.derive_api_key()
            except Exception:
                log.info("Deriving API key failed; creating new one...")
                creds = base.create_api_key(nonce=1)

            self._client = ClobClient(
                host=self.config.CLOB_API,
                key=self.config.PRIVATE_KEY,
                chain_id=POLYGON,
                signature_type=0,
                creds=creds,
            )
            log.info("CLOB client ready — wallet %s…", self._wallet[:10])

        except Exception as exc:
            log.error("CLOB client init failed: %s", exc)
            self._client = None

    # ── Balance ───────────────────────────────────────────────────────────────

    async def get_usdc_balance(self) -> float:
        """USDC balance approved for trading in the CLOB collateral vault."""
        if not self._client:
            return 0.0
        try:
            resp = await asyncio.to_thread(
                self._client.get_balance_allowance,
                params={"asset_type": "USDC"},
            )
            raw = (resp or {}).get("balance", "0")
            return float(raw) / 1_000_000  # USDC has 6 decimals
        except Exception as exc:
            log.warning("Balance check failed: %s", exc)
            return 0.0

    # ── Buy ───────────────────────────────────────────────────────────────────

    async def place_order(self, market: dict, signal) -> OrderResult:
        """
        Place a GTC limit BUY for signal.size USDC worth of signal.token_id
        at signal.price per share.
        """
        if not self._client:
            return OrderResult(success=False, error="CLOB client not ready")

        shares = round(signal.size / signal.price, 4)
        log.info(
            "ORDER BUY  %-50s | %s %.4f shares @ %.4f (~$%.2f)",
            market.get("question", "")[:50], signal.side,
            shares, signal.price, signal.size,
        )

        return await asyncio.to_thread(
            self._place_sync,
            signal.token_id, signal.price, shares, BUY,
        )

    # ── Stop-loss sell ────────────────────────────────────────────────────────

    async def close_position(
        self, token_id: str, shares: float, stop_price: float, question: str = ""
    ) -> OrderResult:
        """
        Place a GTC limit SELL at stop_price to exit a losing position.
        Uses a price 1 % below stop_price to improve fill probability.
        """
        if not self._client:
            return OrderResult(success=False, error="CLOB client not ready")

        fill_price = round(stop_price * 0.99, 4)  # slightly below stop for fill
        log.info(
            "ORDER SELL %-50s | %.4f shares @ %.4f (stop)",
            question[:50], shares, fill_price,
        )

        return await asyncio.to_thread(
            self._place_sync, token_id, fill_price, shares, SELL
        )

    # ── Synchronous helper (runs in thread) ───────────────────────────────────

    def _place_sync(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
    ) -> OrderResult:
        """Blocking order creation + submission. Called via asyncio.to_thread."""
        try:
            order = self._client.create_order(
                OrderArgs(token_id=token_id, price=price, size=size, side=side)
            )
            resp = self._client.post_order(order, OrderType.GTC)

            if resp and resp.get("orderID"):
                return OrderResult(success=True, order_id=resp["orderID"])

            err = (resp or {}).get("errorMsg") or str(resp)
            return OrderResult(success=False, error=err)

        except Exception as exc:
            log.error("Order placement error: %s", exc)
            return OrderResult(success=False, error=str(exc))

    # ── Misc ──────────────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        if not self._client:
            return False
        try:
            await asyncio.to_thread(self._client.cancel, order_id=order_id)
            return True
        except Exception as exc:
            log.warning("Cancel failed for %s: %s", order_id, exc)
            return False
