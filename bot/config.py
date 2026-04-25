"""
Central configuration — all values load from environment / .env.
Defaults are calibrated for a $50 real-money starting balance.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


@dataclass
class Config:
    # ── Wallet ────────────────────────────────────────────────────────────────
    PRIVATE_KEY: str = field(default_factory=lambda: _env("PRIVATE_KEY"))
    POLYGON_RPC: str = field(
        default_factory=lambda: _env("POLYGON_RPC", "https://polygon-rpc.com")
    )

    # ── Telegram ──────────────────────────────────────────────────────────────
    TELEGRAM_TOKEN: str = field(default_factory=lambda: _env("TELEGRAM_TOKEN"))
    TELEGRAM_CHAT_ID: str = field(default_factory=lambda: _env("TELEGRAM_CHAT_ID"))

    # ── Polymarket endpoints ──────────────────────────────────────────────────
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    CLOB_API: str = "https://clob.polymarket.com"
    CHAIN_ID: int = 137  # Polygon mainnet

    # ── Capital & position limits ─────────────────────────────────────────────
    STARTING_BALANCE: float = field(
        default_factory=lambda: _env_float("STARTING_BALANCE", 50.0)
    )
    # Maximum single-trade stake as a fraction of balance
    MAX_POSITION_PCT: float = 0.10          # 10 %
    # Hard dollar cap per trade
    MAX_POSITION_ABS: float = field(
        default_factory=lambda: _env_float("MAX_POSITION_ABS", 5.0)
    )
    # Hard dollar max loss per trade (stop-loss target)
    MAX_LOSS_PER_TRADE: float = 1.0         # $1

    # ── Daily risk limits ─────────────────────────────────────────────────────
    DAILY_LOSS_LIMIT: float = field(
        default_factory=lambda: _env_float("DAILY_LOSS_LIMIT", 2.0)
    )
    MAX_CONSECUTIVE_LOSSES: int = 2

    # ── Edge & fee model ──────────────────────────────────────────────────────
    MIN_EDGE: float = field(
        default_factory=lambda: _env_float("MIN_EDGE", 0.10)
    )
    FEE_RATE: float = 0.02    # 2 % Polymarket fee
    SLIPPAGE: float = 0.005   # 0.5 % estimated slippage
    HALF_KELLY: bool = True

    # ── Market filters ────────────────────────────────────────────────────────
    MIN_VOLUME_24H: float = 30_000.0   # $30 k
    MIN_LIQUIDITY: float = 5_000.0     # $5 k
    MARKET_LIMIT: int = 100            # markets fetched per scan

    # ── EMA mean-reversion model ──────────────────────────────────────────────
    EMA_PERIOD: int = 24               # 24-candle EMA (24 h at 1 h fidelity)
    MIN_DEVIATION: float = 0.08        # 8 % price deviation to signal
    REVERSION_FACTOR: float = 0.35     # assume 35 % mean-reversion

    # ── Stop-loss ─────────────────────────────────────────────────────────────
    # A position is exited when its mark-value drops by MAX_LOSS_PER_TRADE.
    # stop_price = entry * (1 - MAX_LOSS_PER_TRADE / stake)
    # Example: $5 stake → stop at 80 % of entry price.

    # ── Timing ────────────────────────────────────────────────────────────────
    LOOP_INTERVAL: int = field(
        default_factory=lambda: _env_int("LOOP_INTERVAL", 120)
    )

    # ── Storage ───────────────────────────────────────────────────────────────
    DB_PATH: str = "polybot.db"

    # ── Derived helpers ───────────────────────────────────────────────────────
    def total_cost(self) -> float:
        """Combined fee + slippage that must be cleared before a trade pays."""
        return self.FEE_RATE + self.SLIPPAGE

    def required_edge(self) -> float:
        """Minimum raw edge (before fees) that we consider tradeable."""
        return self.MIN_EDGE + self.total_cost()
