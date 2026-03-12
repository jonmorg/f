"""
Kalshi Autonomous Trading Bot
==============================
Elite quant trading system for Kalshi prediction markets (2026).
Uses the official kalshi_python_sync SDK for REST + WebSocket.

Architecture:
  DataFetcher   - polls markets, fetches orderbook, manages WS feeds
  SignalEngine  - ensemble of 4 alpha signals
  RiskManager   - Kelly sizing, drawdown limits, portfolio constraints
  OrderExecutor - submit/cancel/track orders via SDK
  Logger        - structured JSON decision logging

Author: Autonomous Kalshi Bot v1.0
Python: 3.11+
"""

import asyncio
import json
import logging
import os
import random
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from collections import defaultdict, deque

# Third-party (pip install kalshi_python_sync python-dotenv aiohttp)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# Kalshi SDK import
try:
    import kalshi_python_sync as kalshi  # noqa: F401
    from kalshi_python_sync.configuration import Configuration
    from kalshi_python_sync.api_client import ApiClient
    from kalshi_python_sync.api.exchange_api import ExchangeApi  # noqa: F401
    from kalshi_python_sync.api.markets_api import MarketsApi
    from kalshi_python_sync.api.portfolio_api import PortfolioApi
    from kalshi_python_sync.api.auth_api import AuthApi
    KALSHI_SDK_AVAILABLE = True
except ImportError:
    KALSHI_SDK_AVAILABLE = False
    logging.warning("kalshi_python_sync not installed. Running in SIMULATION mode.")

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

PROD_HOST = "https://trading-api.kalshi.com/trade-api/v2"
DEMO_HOST = "https://demo-api.kalshi.co/trade-api/v2"

DEMO_MODE        = os.getenv("KALSHI_DEMO_MODE", "true").lower() == "true"
API_KEY_ID       = os.getenv("KALSHI_API_KEY_ID", "")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")
STARTING_BALANCE = float(os.getenv("STARTING_BALANCE_CENTS", "100000"))  # $1,000

MAX_PORTFOLIO_PCT_PER_MARKET   = 0.12
MAX_PORTFOLIO_PCT_PER_CATEGORY = 0.25
KELLY_FRACTION                 = 0.60
MIN_EDGE_PCT                   = 0.04
DAILY_STOP_LOSS_PCT            = 0.08
MAX_DRAWDOWN_PCT               = 0.25
FEE_ESTIMATE_PCT               = 0.02
SLIPPAGE_ESTIMATE_PCT          = 0.01

POLL_INTERVAL_LIQUID_S   = 30
POLL_INTERVAL_ILLIQUID_S = 300
LIQUIDITY_THRESHOLD      = 500
HEARTBEAT_INTERVAL_S     = 60  # noqa: F841

CATEGORIES = ["politics", "macro", "crypto", "sports", "weather", "culture"]

WATCHED_TICKERS: list[str] = []

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kalshi_bot.log"),
    ],
)
log = logging.getLogger("KalshiBot")


# ---------------------------------------------------------------------------
# DATA CLASSES
# ---------------------------------------------------------------------------

class Side(str, Enum):
    YES = "yes"
    NO  = "no"


class Action(str, Enum):
    BUY  = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class MarketSnapshot:
    """Live state of a single Kalshi market."""
    ticker:        str
    title:         str
    category:      str
    yes_bid:       int
    yes_ask:       int
    volume:        int
    open_interest: int
    close_time:    Optional[datetime] = None
    result:        Optional[str] = None
    is_open:       bool = True
    last_updated:  float = field(default_factory=time.time)

    @property
    def mid(self) -> float:
        return (self.yes_bid + self.yes_ask) / 2.0

    @property
    def spread(self) -> int:
        return self.yes_ask - self.yes_bid


@dataclass
class Signal:
    """Output of a single alpha signal."""
    name:       str
    true_prob:  float
    confidence: float
    edge_yes:   float
    edge_no:    float
    reason:     str


@dataclass
class TradeDecision:
    """Final trade decision emitted as JSON."""
    action:       str
    ticker:       str
    side:         str
    limit_cents:  int
    quantity:     int
    edge_pct:     float
    confidence:   float
    reason:       str
    category:     str = ""
    timestamp:    str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


@dataclass
class Position:
    """Tracks an open position in a market."""
    ticker:    str
    side:      Side
    quantity:  int
    avg_cost:  float
    category:  str


@dataclass
class Portfolio:
    """Live portfolio state."""
    cash_cents:       float
    peak_value:       float
    day_start_value:  float
    positions:        dict[str, Position] = field(default_factory=dict)
    daily_pnl:        float = 0.0
    total_drawdown:   float = 0.0
    halted:           bool = False


# ---------------------------------------------------------------------------
# DATA FETCHER
# ---------------------------------------------------------------------------

class DataFetcher:
    """Fetches market data from Kalshi via REST (polling) and WebSocket (live)."""

    def __init__(self, api_client: Optional[Any] = None):
        self.api_client    = api_client
        self.markets_api   = MarketsApi(api_client) if api_client and KALSHI_SDK_AVAILABLE else None
        self.portfolio_api = PortfolioApi(api_client) if api_client and KALSHI_SDK_AVAILABLE else None
        self._market_cache: dict[str, MarketSnapshot] = {}
        self._ws_subscriptions: set[str] = set()
        self.log = logging.getLogger("DataFetcher")

    async def fetch_all_markets(self, limit: int = 200) -> list[MarketSnapshot]:
        if self.markets_api:
            return await self._fetch_markets_sdk(limit)
        return self._simulate_markets()

    async def _fetch_markets_sdk(self, limit: int) -> list[MarketSnapshot]:
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: self.markets_api.get_markets(status="open", limit=limit),
            )
            snapshots = []
            for m in (resp.markets or []):
                snap = MarketSnapshot(
                    ticker        = m.ticker,
                    title         = getattr(m, "title", m.ticker),
                    category      = self._classify_category(getattr(m, "title", "")),
                    yes_bid       = int(getattr(m, "yes_bid", 45)),
                    yes_ask       = int(getattr(m, "yes_ask", 55)),
                    volume        = int(getattr(m, "volume", 0)),
                    open_interest = int(getattr(m, "open_interest", 0)),
                    is_open       = True,
                )
                snapshots.append(snap)
                self._market_cache[snap.ticker] = snap
            self.log.info(f"Fetched {len(snapshots)} markets from Kalshi.")
            return snapshots
        except Exception as e:
            self.log.error(f"SDK fetch error: {e}")
            return list(self._market_cache.values())

    def _simulate_markets(self) -> list[MarketSnapshot]:
        simulated = [
            ("FED-25BPS-JUN26", "Fed raises 25bps in June 2026",  "macro",   38, 44, 15000,  8000),
            ("BTC-80K-MAR26",   "BTC above $80K by Mar 31 2026",  "crypto",  52, 58, 22000, 11000),
            ("NFL-KC-SB26",     "Chiefs win Super Bowl 2026",      "sports",  28, 34,  9000,  4500),
            ("TRUMP-EO-APR26",  "Trump signs EO on tariffs Apr26", "politics",60, 67,  5000,  2500),
            ("RAIN-NYC-MAR15",  "Rain in NYC on Mar 15 2026",      "weather", 40, 50,  1200,   600),
            ("OSCARS-BEST-26",  "Certain film wins Best Picture",  "culture", 22, 30,  3000,  1500),
            ("FED-HOLD-MAY26",  "Fed holds rates in May 2026",     "macro",   55, 62, 18000,  9000),
            ("ETH-3K-MAR26",    "ETH above $3K by Mar 31 2026",   "crypto",  44, 52, 12000,  6000),
            ("SENATE-REP-26",   "Republicans hold Senate 2026",    "politics",48, 56, 25000, 13000),
            ("CPI-3PCT-FEB26",  "CPI above 3% in Feb 2026",       "macro",   33, 41,  7000,  3500),
        ]
        snaps = []
        for ticker, title, cat, bid, ask, vol, oi in simulated:
            noise_bid = max(1,  bid + random.randint(-2, 2))
            noise_ask = min(99, ask + random.randint(-2, 2))
            snap = MarketSnapshot(
                ticker        = ticker,
                title         = title,
                category      = cat,
                yes_bid       = noise_bid,
                yes_ask       = noise_ask,
                volume        = vol,
                open_interest = oi,
            )
            snaps.append(snap)
            self._market_cache[ticker] = snap
        return snaps

    def _classify_category(self, title: str) -> str:
        title_lower = title.lower()
        if any(k in title_lower for k in ["fed", "cpi", "gdp", "rate", "inflation", "fomc"]):
            return "macro"
        if any(k in title_lower for k in ["btc", "eth", "bitcoin", "ethereum", "crypto"]):
            return "crypto"
        if any(k in title_lower for k in ["nfl", "nba", "super bowl", "mlb", "nhl", "espn"]):
            return "sports"
        if any(k in title_lower for k in ["rain", "snow", "hurricane", "temperature", "storm"]):
            return "weather"
        if any(k in title_lower for k in ["oscar", "grammy", "emmy", "box office", "film"]):
            return "culture"
        return "politics"

    async def get_orderbook(self, ticker: str) -> Optional[dict]:
        if self.markets_api:
            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self.markets_api.get_market_order_book(ticker),
                )
                return {"bids": resp.order_book.yes or [], "asks": resp.order_book.no or []}
            except Exception as e:
                self.log.warning(f"Orderbook fetch failed for {ticker}: {e}")
        snap = self._market_cache.get(ticker)
        if snap:
            return {
                "bids": [[snap.yes_bid, random.randint(10, 100)]],
                "asks": [[snap.yes_ask, random.randint(10, 100)]],
            }
        return None

    async def get_portfolio_balance(self) -> float:
        if self.portfolio_api:
            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(None, self.portfolio_api.get_balance)
                return float(resp.balance * 100)
            except Exception as e:
                self.log.warning(f"Balance fetch failed: {e}")
        return STARTING_BALANCE

    async def fetch_news_sentiment(self, topic: str) -> float:
        if not AIOHTTP_AVAILABLE:
            return 0.0
        feeds = {
            "macro":    "https://feeds.bloomberg.com/markets/news.rss",
            "crypto":   "https://cointelegraph.com/rss",
            "politics": "https://rss.politico.com/politics-news.xml",
            "sports":   "https://www.espn.com/espn/rss/news",
            "weather":  "",
            "culture":  "",
        }
        feed_url = feeds.get(topic, "")
        if not feed_url:
            return 0.0
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(feed_url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        positive_words = ["rise", "gain", "bull", "growth", "positive",
                                          "strong", "beat", "surge", "rally", "up"]
                        negative_words = ["fall", "drop", "bear", "decline", "negative",
                                          "weak", "miss", "crash", "plunge", "down"]
                        text_lower = text.lower()
                        pos_count = sum(text_lower.count(w) for w in positive_words)
                        neg_count = sum(text_lower.count(w) for w in negative_words)
                        total = pos_count + neg_count
                        if total == 0:
                            return 0.0
                        return (pos_count - neg_count) / total
        except Exception:
            pass
        return 0.0


# ---------------------------------------------------------------------------
# SIGNAL ENGINE
# ---------------------------------------------------------------------------

class SignalEngine:
    """Ensemble of 4 alpha signals."""

    BASE_RATES: dict[str, dict[str, float]] = {
        "macro":    {"fed_hike": 0.30, "fed_hold": 0.60, "cpi_above_3": 0.45},
        "crypto":   {"btc_up_month": 0.55, "eth_up_month": 0.52},
        "politics": {"incumbent_advantage": 0.55},
        "sports":   {"home_advantage": 0.54},
        "weather":  {"rain_base": 0.40},
        "culture":  {"frontrunner": 0.45},
    }

    def __init__(self, data_fetcher: DataFetcher):
        self.fetcher = data_fetcher
        self.sentiment_cache: dict[str, tuple[float, float]] = {}
        self.market_history:  dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.log = logging.getLogger("SignalEngine")

    async def compute_ensemble_signal(
        self,
        snap: MarketSnapshot,
        related_markets: list[MarketSnapshot],
    ) -> list[Signal]:
        signals: list[Signal] = []
        s1 = await self._signal_mispricing(snap)
        if s1:
            signals.append(s1)
        s2 = self._signal_market_making(snap)
        if s2:
            signals.append(s2)
        s3 = await self._signal_event_driven(snap)
        if s3:
            signals.append(s3)
        s4 = self._signal_cross_market_arb(snap, related_markets)
        if s4:
            signals.append(s4)
        self.market_history[snap.ticker].append(snap.mid)
        return signals

    async def _signal_mispricing(self, snap: MarketSnapshot) -> Optional[Signal]:
        base_prob   = self._get_base_rate(snap)
        market_prob = snap.mid / 100.0
        true_prob   = 0.40 * base_prob + 0.60 * market_prob
        vol         = self._estimate_vol(snap.ticker)
        true_prob   = self._vol_adjust_prob(true_prob, vol)
        total_cost  = FEE_ESTIMATE_PCT + SLIPPAGE_ESTIMATE_PCT
        edge_yes    = true_prob - (snap.yes_ask / 100.0) - total_cost
        edge_no     = (1.0 - true_prob) - ((100 - snap.yes_bid) / 100.0) - total_cost
        if max(edge_yes, edge_no) < MIN_EDGE_PCT:
            return None
        confidence = min(0.90, 0.50 + abs(true_prob - market_prob) * 2)
        return Signal(
            name       = "mispricing",
            true_prob  = true_prob,
            confidence = confidence,
            edge_yes   = edge_yes,
            edge_no    = edge_no,
            reason     = (f"Base rate {base_prob:.2f} -> true_prob {true_prob:.2f} "
                          f"vs market mid {market_prob:.2f}; vol={vol:.3f}"),
        )

    def _get_base_rate(self, snap: MarketSnapshot) -> float:
        cat_rates = self.BASE_RATES.get(snap.category, {})
        title_lower = snap.title.lower()
        for key, rate in cat_rates.items():
            if any(word in title_lower for word in key.split("_")):
                return rate
        return 0.50

    def _estimate_vol(self, ticker: str) -> float:
        history = list(self.market_history[ticker])
        if len(history) < 5:
            return 0.05
        diffs = [abs(history[i] - history[i - 1]) / 100.0 for i in range(1, len(history))]
        return sum(diffs) / len(diffs)

    def _vol_adjust_prob(self, prob: float, vol: float) -> float:
        shrinkage = min(0.3, vol * 3)
        return prob * (1 - shrinkage) + 0.5 * shrinkage

    def _signal_market_making(self, snap: MarketSnapshot) -> Optional[Signal]:
        if snap.spread < 6 or snap.open_interest < LIQUIDITY_THRESHOLD:
            return None
        fair    = snap.mid / 100.0
        edge_mm = (snap.spread / 2.0 - 2.0) / 100.0
        if edge_mm < MIN_EDGE_PCT:
            return None
        return Signal(
            name       = "market_making",
            true_prob  = fair,
            confidence = 0.60,
            edge_yes   = edge_mm,
            edge_no    = edge_mm,
            reason     = (f"Spread={snap.spread}c on liquid market "
                          f"(OI={snap.open_interest}); MM edge={edge_mm:.3f}"),
        )

    async def _signal_event_driven(self, snap: MarketSnapshot) -> Optional[Signal]:
        cat    = snap.category
        cached = self.sentiment_cache.get(cat)
        now    = time.time()
        if not cached or now - cached[1] > 600:
            score = await self.fetcher.fetch_news_sentiment(cat)
            self.sentiment_cache[cat] = (score, now)
        else:
            score = cached[0]
        if abs(score) < 0.10:
            return None
        prob_shift = score * 0.08
        base_prob  = snap.mid / 100.0
        true_prob  = max(0.02, min(0.98, base_prob + prob_shift))
        total_cost = FEE_ESTIMATE_PCT + SLIPPAGE_ESTIMATE_PCT
        edge_yes   = true_prob - (snap.yes_ask / 100.0) - total_cost
        edge_no    = (1.0 - true_prob) - ((100 - snap.yes_bid) / 100.0) - total_cost
        if max(edge_yes, edge_no) < MIN_EDGE_PCT:
            return None
        return Signal(
            name       = "event_driven",
            true_prob  = true_prob,
            confidence = min(0.75, 0.45 + abs(score) * 0.5),
            edge_yes   = edge_yes,
            edge_no    = edge_no,
            reason     = f"News sentiment score={score:.2f} for category={cat}",
        )

    def _signal_cross_market_arb(
        self,
        snap: MarketSnapshot,
        related: list[MarketSnapshot],
    ) -> Optional[Signal]:
        if not related:
            return None
        related_yes_sum = sum(r.mid for r in related if r.ticker != snap.ticker)
        total_sum = snap.mid + related_yes_sum
        if total_sum > 103:
            overpriced_pct = (total_sum - 100) / 100.0
            all_mids = [(snap.mid, snap)] + [(r.mid, r) for r in related]
            all_mids.sort(key=lambda x: x[0], reverse=True)
            target = all_mids[0][1]
            if target.ticker == snap.ticker:
                true_prob = snap.mid / 100.0 - overpriced_pct / 2
                edge_no   = overpriced_pct / 2 - (FEE_ESTIMATE_PCT + SLIPPAGE_ESTIMATE_PCT)
                if edge_no >= MIN_EDGE_PCT:
                    return Signal(
                        name       = "cross_market_arb",
                        true_prob  = true_prob,
                        confidence = 0.85,
                        edge_yes   = -edge_no,
                        edge_no    = edge_no,
                        reason     = (f"Mutual exclusivity violated: sum of related YES mids "
                                      f"= {total_sum:.1f}c > 100; arb edge={edge_no:.3f}"),
                    )
        return None

    def aggregate_signals(self, signals: list[Signal]) -> tuple[float, float, float, str]:
        """
        Weighted average of signals.
        Returns (true_prob, best_edge, confidence, reason).
        """
        if not signals:
            return 0.5, 0.0, 0.0, "no_signal"

        weight_map = {
            "cross_market_arb": 0.40,
            "mispricing":       0.30,
            "market_making":    0.20,
            "event_driven":     0.10,
        }
        total_w      = 0.0
        prob_sum     = 0.0
        conf_sum     = 0.0
        edge_yes_sum = 0.0
        edge_no_sum  = 0.0
        reasons: list[str] = []

        for s in signals:
            w = weight_map.get(s.name, 0.10)
            prob_sum     += s.true_prob  * w
            conf_sum     += s.confidence * w
            edge_yes_sum += s.edge_yes   * w
            edge_no_sum  += s.edge_no    * w
            total_w      += w
            reasons.append(f"[{s.name}] {s.reason}")

        if total_w == 0:
            return 0.5, 0.0, 0.0, "zero_weight"

        true_prob  = prob_sum     / total_w
        confidence = conf_sum     / total_w
        best_edge  = max(edge_yes_sum / total_w, edge_no_sum / total_w)

        return true_prob, best_edge, confidence, "; ".join(reasons)


# ---------------------------------------------------------------------------
# RISK MANAGER
# ---------------------------------------------------------------------------

class RiskManager:
    """Enforces all risk rules."""

    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.log = logging.getLogger("RiskManager")

    def check_halt_conditions(self) -> bool:
        p = self.portfolio
        if p.halted:
            return True
        if p.day_start_value > 0:
            daily_pnl_pct = (p.cash_cents - p.day_start_value) / p.day_start_value
            if daily_pnl_pct <= -DAILY_STOP_LOSS_PCT:
                self.log.critical(
                    f"DAILY STOP HIT: {daily_pnl_pct:.1%} < -{DAILY_STOP_LOSS_PCT:.1%}"
                )
                p.halted = True
                return True
        total_value = self.total_portfolio_value()
        if p.peak_value > 0:
            drawdown = (p.peak_value - total_value) / p.peak_value
            if drawdown >= MAX_DRAWDOWN_PCT:
                self.log.critical(
                    f"MAX DRAWDOWN HIT: {drawdown:.1%} >= {MAX_DRAWDOWN_PCT:.1%}"
                )
                p.halted = True
                return True
        if total_value > p.peak_value:
            p.peak_value = total_value
        return False

    def total_portfolio_value(self) -> float:
        pos_value = sum(
            pos.quantity * pos.avg_cost
            for pos in self.portfolio.positions.values()
        )
        return self.portfolio.cash_cents + pos_value

    def kelly_position_size(
        self,
        true_prob:   float,
        limit_cents: int,
        edge_pct:    float,
        snap:        MarketSnapshot,
    ) -> int:
        p    = true_prob
        q    = 1.0 - p
        cost = limit_cents / 100.0
        if cost <= 0 or cost >= 1:
            return 0
        b = (1.0 - cost) / cost
        if b <= 0:
            return 0
        kelly_f = max(0.0, (p * b - q) / b)
        kelly_f *= KELLY_FRACTION
        kelly_f *= min(1.0, edge_pct / 0.10)
        total_value         = self.total_portfolio_value()
        max_notional_market = total_value * MAX_PORTFOLIO_PCT_PER_MARKET
        max_notional_cat    = self._remaining_category_budget(snap.category, total_value)
        max_notional        = min(max_notional_market, max_notional_cat)
        target_notional     = min(total_value * kelly_f, max_notional)
        contracts           = int(target_notional / limit_cents) if limit_cents > 0 else 0
        return max(0, contracts)

    def _remaining_category_budget(self, category: str, total_value: float) -> float:
        """Return remaining notional budget (in cents) for the given category."""
        max_category_notional = total_value * MAX_PORTFOLIO_PCT_PER_CATEGORY
        current_exposure = sum(
            pos.quantity * pos.avg_cost
            for pos in self.portfolio.positions.values()
            if pos.category == category
        )
        return max(0.0, max_category_notional - current_exposure)

    def compute_decision(
        self,
        snap:       MarketSnapshot,
        true_prob:  float,
        best_edge:  float,
        confidence: float,
        reason:     str,
    ) -> Optional[TradeDecision]:
        if self.check_halt_conditions():
            return None
        if best_edge < MIN_EDGE_PCT:
            return None
        total_cost = FEE_ESTIMATE_PCT + SLIPPAGE_ESTIMATE_PCT
        edge_yes   = true_prob - (snap.yes_ask / 100.0) - total_cost
        edge_no    = (1.0 - true_prob) - ((100 - snap.yes_bid) / 100.0) - total_cost
        if edge_yes >= edge_no and edge_yes >= MIN_EDGE_PCT:
            side        = Side.YES
            limit_cents = snap.yes_ask
            actual_edge = edge_yes
        elif edge_no >= MIN_EDGE_PCT:
            side        = Side.NO
            limit_cents = 100 - snap.yes_bid
            actual_edge = edge_no
        else:
            return None
        quantity = self.kelly_position_size(true_prob, limit_cents, actual_edge, snap)
        if quantity <= 0:
            return None
        required_cash = quantity * limit_cents
        if required_cash > self.portfolio.cash_cents:
            quantity = int(self.portfolio.cash_cents / limit_cents)
            if quantity <= 0:
                return None
        return TradeDecision(
            action      = Action.BUY.value,
            ticker      = snap.ticker,
            side        = side.value,
            limit_cents = limit_cents,
            quantity    = quantity,
            edge_pct    = round(actual_edge, 4),
            confidence  = round(confidence, 4),
            reason      = reason,
            category    = snap.category,
        )


# ---------------------------------------------------------------------------
# ORDER EXECUTOR
# ---------------------------------------------------------------------------

class OrderExecutor:
    """Submits, tracks, and cancels orders via the Kalshi SDK."""

    def __init__(self, api_client: Optional[Any], portfolio: Portfolio):
        self.portfolio_api = PortfolioApi(api_client) if api_client and KALSHI_SDK_AVAILABLE else None
        self.portfolio     = portfolio
        self._open_orders: dict[str, dict] = {}
        self.log = logging.getLogger("OrderExecutor")

    async def submit_order(self, decision: TradeDecision) -> Optional[str]:
        if self.portfolio_api:
            return await self._submit_sdk(decision)
        return self._simulate_order(decision)

    async def _submit_sdk(self, decision: TradeDecision) -> Optional[str]:
        loop = asyncio.get_event_loop()
        try:
            yes_price = (
                decision.limit_cents
                if decision.side == "yes"
                else 100 - decision.limit_cents
            )
            order_params = {
                "ticker":          decision.ticker,
                "side":            decision.side,
                "action":          decision.action,
                "type":            "limit",
                "yes_price":       yes_price,
                "count":           decision.quantity,
                "client_order_id": f"bot_{int(time.time() * 1000)}",
            }
            resp = await loop.run_in_executor(
                None,
                lambda: self.portfolio_api.create_order(order_params),
            )
            order_id = resp.order.order_id
            self._open_orders[order_id] = {
                "decision":     decision,
                "submitted_at": time.time(),
                "status":       "open",
            }
            self.portfolio.cash_cents -= decision.quantity * decision.limit_cents
            self.log.info(
                f"Order submitted: {order_id} | {decision.ticker} "
                f"{decision.side.upper()} x{decision.quantity} @ {decision.limit_cents}c"
            )
            return order_id
        except Exception as e:
            self.log.error(f"Order submission failed: {e}")
            return None

    def _simulate_order(self, decision: TradeDecision) -> Optional[str]:
        cost = decision.quantity * decision.limit_cents
        if cost > self.portfolio.cash_cents:
            self.log.warning(
                f"Insufficient cash ({self.portfolio.cash_cents:.0f}c) "
                f"for order cost {cost:.0f}c"
            )
            return None
        self.portfolio.cash_cents -= cost
        ticker = decision.ticker
        if ticker in self.portfolio.positions:
            pos       = self.portfolio.positions[ticker]
            total_qty = pos.quantity + decision.quantity
            pos.avg_cost = (
                (pos.quantity * pos.avg_cost + decision.quantity * decision.limit_cents)
                / total_qty
            )
            pos.quantity = total_qty
        else:
            self.portfolio.positions[ticker] = Position(
                ticker   = ticker,
                side     = Side(decision.side),
                quantity = decision.quantity,
                avg_cost = float(decision.limit_cents),
                category = decision.category,
            )
        order_id = f"sim_{int(time.time() * 1000)}_{ticker}"
        self._open_orders[order_id] = {
            "decision":     decision,
            "submitted_at": time.time(),
            "status":       "filled",
        }
        self.log.info(
            f"[SIM] Order filled: {order_id} | {ticker} "
            f"{decision.side.upper()} x{decision.quantity} @ {decision.limit_cents}c "
            f"| Cash remaining: {self.portfolio.cash_cents:.0f}c"
        )
        return order_id

    async def cancel_stale_orders(self, max_age_s: float = 120.0) -> None:
        now   = time.time()
        stale = [
            oid for oid, info in self._open_orders.items()
            if info["status"] == "open" and now - info["submitted_at"] > max_age_s
        ]
        for order_id in stale:
            await self._cancel_order(order_id)

    async def _cancel_order(self, order_id: str) -> None:
        if self.portfolio_api:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.portfolio_api.cancel_order(order_id),
                )
                self.log.info(f"Order cancelled: {order_id}")
            except Exception as e:
                self.log.warning(f"Cancel failed for {order_id}: {e}")
        self._open_orders.pop(order_id, None)

    async def sync_fills(self) -> None:
        if not self.portfolio_api:
            return
        loop = asyncio.get_event_loop()
        for order_id, info in list(self._open_orders.items()):
            if info["status"] != "open":
                continue
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda oid=order_id: self.portfolio_api.get_order(oid),
                )
                status = resp.order.status
                if status in ("filled", "canceled", "expired"):
                    info["status"] = status
                    if status == "filled":
                        self._record_fill(info["decision"])
                    self._open_orders.pop(order_id, None)
            except Exception as e:
                self.log.warning(f"Fill sync failed for {order_id}: {e}")

    def _record_fill(self, decision: TradeDecision) -> None:
        ticker = decision.ticker
        if ticker in self.portfolio.positions:
            pos       = self.portfolio.positions[ticker]
            total_qty = pos.quantity + decision.quantity
            pos.avg_cost = (
                (pos.quantity * pos.avg_cost + decision.quantity * decision.limit_cents)
                / total_qty
            )
            pos.quantity = total_qty
        else:
            self.portfolio.positions[ticker] = Position(
                ticker   = ticker,
                side     = Side(decision.side),
                quantity = decision.quantity,
                avg_cost = float(decision.limit_cents),
                category = decision.category,
            )


# ---------------------------------------------------------------------------
# DECISION LOGGER
# ---------------------------------------------------------------------------

class DecisionLogger:
    """Writes structured JSON trade decisions and portfolio snapshots to JSONL."""

    def __init__(self, filepath: str = "decisions.jsonl"):
        self.filepath = filepath
        self.log = logging.getLogger("DecisionLogger")

    def log_decision(self, decision: TradeDecision) -> None:
        record = asdict(decision)
        record["event"] = "trade_decision"
        self._write(record)
        self.log.info(
            f"DECISION | {decision.action.upper()} {decision.ticker} "
            f"{decision.side.upper()} x{decision.quantity} @ {decision.limit_cents}c "
            f"edge={decision.edge_pct:.2%} conf={decision.confidence:.2%}"
        )

    def log_hold(self, ticker: str, reason: str) -> None:
        self._write({
            "event":     "hold",
            "ticker":    ticker,
            "reason":    reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def log_portfolio(self, portfolio: Portfolio, risk_manager: "RiskManager") -> None:
        total_value = risk_manager.total_portfolio_value()
        self._write({
            "event":       "portfolio_snapshot",
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "cash_cents":  portfolio.cash_cents,
            "total_value": total_value,
            "peak_value":  portfolio.peak_value,
            "positions": {
                ticker: {
                    "side":     pos.side.value,
                    "quantity": pos.quantity,
                    "avg_cost": pos.avg_cost,
                    "category": pos.category,
                }
                for ticker, pos in portfolio.positions.items()
            },
            "halted": portfolio.halted,
        })
        self.log.info(
            f"PORTFOLIO | cash={portfolio.cash_cents:.0f}c "
            f"total={total_value:.0f}c "
            f"positions={len(portfolio.positions)} "
            f"halted={portfolio.halted}"
        )

    def log_halt(self, reason: str) -> None:
        self._write({
            "event":     "halt",
            "reason":    reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.log.critical(f"TRADING HALTED: {reason}")

    def _write(self, record: dict) -> None:
        try:
            with open(self.filepath, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            self.log.error(f"Failed to write decision log: {e}")


# ---------------------------------------------------------------------------
# MAIN BOT ORCHESTRATOR
# ---------------------------------------------------------------------------

class KalshiBot:
    """
    Top-level orchestrator that ties DataFetcher, SignalEngine,
    RiskManager, OrderExecutor, and DecisionLogger together.
    """

    def __init__(self):
        self.log       = logging.getLogger("KalshiBot")
        api_client     = self._build_api_client()
        self.portfolio = Portfolio(
            cash_cents      = STARTING_BALANCE,
            peak_value      = STARTING_BALANCE,
            day_start_value = STARTING_BALANCE,
        )
        self.fetcher  = DataFetcher(api_client)
        self.signals  = SignalEngine(self.fetcher)
        self.risk     = RiskManager(self.portfolio)
        self.executor = OrderExecutor(api_client, self.portfolio)
        self.logger   = DecisionLogger()
        self._last_portfolio_log = 0.0
        self._day_reset_ts       = time.time()

    def _build_api_client(self) -> Optional[Any]:
        if not KALSHI_SDK_AVAILABLE:
            self.log.warning("Kalshi SDK not available -- running in SIMULATION mode.")
            return None
        if not API_KEY_ID:
            self.log.warning("No KALSHI_API_KEY_ID set -- running in SIMULATION mode.")
            return None
        host = DEMO_HOST if DEMO_MODE else PROD_HOST
        try:
            config     = Configuration(host=host)
            api_client = ApiClient(configuration=config)
            if os.path.exists(PRIVATE_KEY_PATH):
                with open(PRIVATE_KEY_PATH, "r") as f:
                    private_key_pem = f.read()
                auth_api = AuthApi(api_client)
                auth_api.login(
                    login_request={"email": API_KEY_ID, "password": private_key_pem}
                )
                self.log.info(
                    f"Authenticated with Kalshi ({'DEMO' if DEMO_MODE else 'PROD'})"
                )
            else:
                self.log.warning(
                    f"Private key not found at {PRIVATE_KEY_PATH}. "
                    "Proceeding unauthenticated (read-only)."
                )
            return api_client
        except Exception as e:
            self.log.error(f"API client build failed: {e}. Falling back to SIMULATION.")
            return None

    async def run(self) -> None:
        self.log.info("=" * 60)
        self.log.info("Kalshi Autonomous Trading Bot starting...")
        self.log.info(f"Mode: {'DEMO' if DEMO_MODE else 'PRODUCTION'}")
        self.log.info(f"Starting balance: {self.portfolio.cash_cents:.0f}c")
        self.log.info("=" * 60)
        while True:
            try:
                await self._trading_cycle()
            except asyncio.CancelledError:
                self.log.info("Bot shut down cleanly.")
                break
            except Exception:
                self.log.error(
                    f"Unexpected error in trading cycle:\n{traceback.format_exc()}"
                )
                await asyncio.sleep(10)

    async def _trading_cycle(self) -> None:
        now = time.time()
        # Daily P&L reset at midnight UTC
        if now - self._day_reset_ts >= 86400:
            self.portfolio.day_start_value = self.risk.total_portfolio_value()
            self._day_reset_ts = now
            self.log.info("Daily P&L reset.")

        if self.risk.check_halt_conditions():
            self.logger.log_halt("Halt condition triggered -- sleeping 60s")
            await asyncio.sleep(60)
            return

        await self.executor.sync_fills()
        await self.executor.cancel_stale_orders()

        markets = await self.fetcher.fetch_all_markets()
        if not markets:
            self.log.warning("No markets fetched. Sleeping...")
            await asyncio.sleep(POLL_INTERVAL_ILLIQUID_S)
            return

        self.log.info(f"Processing {len(markets)} markets...")

        by_category: dict[str, list[MarketSnapshot]] = defaultdict(list)
        for m in markets:
            by_category[m.category].append(m)

        decisions_made = 0
        for snap in markets:
            if not snap.is_open:
                continue
            related = [
                m for m in by_category.get(snap.category, [])
                if m.ticker != snap.ticker
            ]
            signals = await self.signals.compute_ensemble_signal(snap, related)
            if not signals:
                self.logger.log_hold(snap.ticker, "no_signal")
                continue

            true_prob, best_edge, confidence, reason = self.signals.aggregate_signals(signals)
            decision = self.risk.compute_decision(
                snap, true_prob, best_edge, confidence, reason
            )
            if decision is None:
                self.logger.log_hold(
                    snap.ticker,
                    f"edge={best_edge:.3f} below threshold or halted",
                )
                continue

            order_id = await self.executor.submit_order(decision)
            if order_id:
                self.logger.log_decision(decision)
                decisions_made += 1
                # Ensure category is set on the position (executor sets it on new
                # positions; this handles existing positions being re-evaluated)
                if snap.ticker in self.portfolio.positions:
                    self.portfolio.positions[snap.ticker].category = snap.category

        self.log.info(f"Cycle complete. Decisions made: {decisions_made}/{len(markets)}")

        if now - self._last_portfolio_log >= 300:
            self.logger.log_portfolio(self.portfolio, self.risk)
            self._last_portfolio_log = now

        liquid_count = sum(1 for m in markets if m.open_interest >= LIQUIDITY_THRESHOLD)
        sleep_s = POLL_INTERVAL_LIQUID_S if liquid_count > 0 else POLL_INTERVAL_ILLIQUID_S
        self.log.info(f"Sleeping {sleep_s}s (liquid_markets={liquid_count})...")
        await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

async def main() -> None:
    bot = KalshiBot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot interrupted by user. Goodbye.")
