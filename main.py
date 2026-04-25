"""
Polymarket Value Betting Bot — main entry point.

Run: python main.py
"""

import asyncio
import logging
import sys
from datetime import datetime

from bot.config import Config
from bot.database import Database
from bot.market_fetcher import MarketFetcher
from bot.edge_calculator import EdgeCalculator
from bot.risk_manager import RiskManager
from bot.order_manager import OrderManager
from bot.telegram_alerts import TelegramAlerter

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("polybot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

BANNER = r"""
╔══════════════════════════════════════════════════════════════╗
║           POLYMARKET VALUE BETTING BOT  v1.0                ║
╠══════════════════════════════════════════════════════════════╣
║  WARNING: Running with REAL $50.  Max risk $1 per trade.    ║
║  Position: $0.50 – $5.00   Daily loss limit: $2.00          ║
║  Circuit breaker: stops after 2 consecutive losses          ║
║  Scan loop: every 2 minutes                                  ║
╚══════════════════════════════════════════════════════════════╝
"""


# ── Cycle logic ───────────────────────────────────────────────────────────────

async def run_stop_loss_checks(
    risk_mgr: RiskManager,
    fetcher: MarketFetcher,
    order_mgr: OrderManager,
    db: Database,
    alerter: TelegramAlerter,
):
    """Exit any open positions whose price has fallen to their stop level."""
    positions_to_stop = await risk_mgr.get_positions_to_stop(fetcher)
    for pos in positions_to_stop:
        result = await order_mgr.close_position(
            pos.token_id, pos.shares, pos.stop_price, pos.question
        )
        db.close_position(pos.order_id, reason="STOP_LOSS")
        # Record an estimated loss
        estimated_loss = -(pos.size_usdc - pos.shares * pos.stop_price)
        db.settle_trade(pos.order_id, estimated_loss)
        await alerter.send_stop_loss(pos, result)
        log.info("Stop-loss executed for order %s", pos.order_id)


async def run_scan_cycle(
    config: Config,
    db: Database,
    fetcher: MarketFetcher,
    edge_calc: EdgeCalculator,
    risk_mgr: RiskManager,
    order_mgr: OrderManager,
    alerter: TelegramAlerter,
):
    """One full scan-and-trade cycle."""

    # ── Daily risk gate ───────────────────────────────────────────────────────
    if not risk_mgr.can_trade():
        reason = risk_mgr.stop_reason()
        log.info("Trading paused — %s", reason)
        await alerter.send_risk_halt(reason)
        return

    # ── Fetch markets ─────────────────────────────────────────────────────────
    log.info("Fetching markets…")
    markets = await fetcher.get_active_markets()
    log.info("Scanning %d qualifying markets", len(markets))

    if not markets:
        log.warning("No markets returned — API may be slow.")
        return

    # ── Evaluate edge for each market ─────────────────────────────────────────
    opportunities = []
    for market in markets:
        try:
            signal = await edge_calc.analyze_market(market)
            if signal:
                db.log_signal(signal, market, traded=False)  # mark as candidate
                opportunities.append((market, signal))
        except Exception as exc:
            log.warning("Edge calc error [%s]: %s", market.get("id", "?")[:12], exc)

    if not opportunities:
        log.info("No edge opportunities found this cycle.")
        return

    log.info("Found %d opportunity/ies — picking best edge.", len(opportunities))

    # Take the single best opportunity per cycle (conservative for small capital)
    opportunities.sort(key=lambda x: x[1].edge, reverse=True)
    market, signal = opportunities[0]

    # ── Final size clamp ──────────────────────────────────────────────────────
    signal.size = risk_mgr.size_position(signal.size)
    if signal.size < 0.50:
        log.info("Position too small after risk clamp (${:.2f}), skipping.", signal.size)
        return

    stop_price = risk_mgr.compute_stop_price(signal.price, signal.size)

    # ── Send pre-trade alert ──────────────────────────────────────────────────
    await alerter.send_signal(market, signal)

    # ── Place order ───────────────────────────────────────────────────────────
    log.info(
        "Placing %s order: $%.2f @ %.4f | edge=%.1f%%",
        signal.side, signal.size, signal.price, signal.edge * 100,
    )

    result = await order_mgr.place_order(market, signal)

    if result.success:
        db.log_trade(result, market, signal, stop_price)
        db.log_signal(signal, market, traded=True)
        db.open_position(result, market, signal, stop_price)
        await alerter.send_trade(market, signal, result, stop_price)
        log.info("Order placed successfully — ID: %s", result.order_id)
    else:
        log.error("Order FAILED: %s", result.error)
        db.log_event("ORDER_FAILED", f"{market['question'][:60]} | {result.error}")
        await alerter.send(f"Order failed: {result.error}")


# ── Main loop ─────────────────────────────────────────────────────────────────

async def main():
    print(BANNER)

    config = Config()
    db = Database(config.DB_PATH)
    fetcher = MarketFetcher(config)
    edge_calc = EdgeCalculator(config, fetcher)
    risk_mgr = RiskManager(db, config)
    order_mgr = OrderManager(config)
    alerter = TelegramAlerter(config)

    # Hard check — exit immediately if wallet isn't configured
    if not config.PRIVATE_KEY:
        log.error("PRIVATE_KEY is not set in .env — cannot trade. Exiting.")
        sys.exit(1)

    # Startup balance probe
    balance = await order_mgr.get_usdc_balance()
    log.info("CLOB USDC balance: $%.4f", balance)
    if balance > 0:
        edge_calc.update_bankroll(balance)

    db.log_event(
        "STARTUP",
        f"Bot started | balance=${balance:.4f} | loop={config.LOOP_INTERVAL}s",
    )

    startup_msg = (
        f"Polymarket bot started.\n"
        f"CLOB balance: ${balance:.4f}\n"
        f"Max bet: ${config.MAX_POSITION_ABS:.2f} | "
        f"Max loss/trade: ${config.MAX_LOSS_PER_TRADE:.2f} | "
        f"Daily limit: ${config.DAILY_LOSS_LIMIT:.2f}\n"
        f"Min edge: {config.MIN_EDGE:.0%} | Loop: {config.LOOP_INTERVAL}s"
    )
    log.info(startup_msg)
    await alerter.send(startup_msg)

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.utcnow().strftime("%H:%M:%S")
        log.info("═══ Cycle %d  @ %s UTC ═══", cycle, ts)

        try:
            # 1. Check and execute any stop-loss exits first
            await run_stop_loss_checks(risk_mgr, fetcher, order_mgr, db, alerter)

            # 2. Scan for new opportunities
            await run_scan_cycle(
                config, db, fetcher, edge_calc,
                risk_mgr, order_mgr, alerter,
            )

        except Exception as exc:
            log.exception("Unhandled error in cycle %d: %s", cycle, exc)
            db.log_event("ERROR", str(exc))
            await alerter.send(f"Unhandled error (cycle {cycle}): {exc}")

        # Hourly balance refresh + daily summary at midnight
        if cycle % 30 == 0:
            balance = await order_mgr.get_usdc_balance()
            edge_calc.update_bankroll(balance)
            stats = db.get_today_stats()
            await alerter.send_daily_summary(stats, balance)
            log.info("Balance refresh: $%.4f", balance)

        await asyncio.sleep(config.LOOP_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
