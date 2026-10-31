# language: Python 3.11+
# file: final3_pro.py
# Final-3 Pro — Single File | Dynamic size + light MTF + 7% SL + 1 scale-in

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from collections import deque
import logging

import ccxt.async_support as ccxt
import pandas as pd

# ====================== CONFIG ======================
LEVERAGE = 50
BASE_MARGIN_PCT = 0.10
MAX_MARGIN_PCT = 0.17
CASCADE_THRESHOLD = 0.012
VOLUME_SPIKE = 3.5
TAKE_PROFIT_PCT = 0.018
STOP_LOSS_PCT = 0.07
COOLDOWN_SEC = 8
SCAN_INTERVAL = 600
MAX_SYMBOLS = 40
MAX_POSITIONS = 5
MIN_QUOTE_VOLUME = 15_000_000
ADD_LEVEL = 0.025
FEE_RATE = 0.0005
STATE_FILE = Path("paper_state_final3_pro.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("FINAL3-PRO")

# ====================== PAPER ACCOUNT ======================
class PaperAccount:
    def __init__(self, fee_rate=FEE_RATE):
        self.fee_rate = fee_rate
        self.balance = 100.0
        self.positions = {}
        self.prices = {}
        self.trades = []

    @property
    def used(self):
        return sum(p.get("margin", 0) for p in self.positions.values())

    @property
    def free(self):
        return max(0.0, self.balance - self.used)

    @property
    def equity(self):
        pnl = 0.0
        for s, p in self.positions.items():
            price = self.prices.get(s, p["entry"])
            direction = 1 if p["side"] == "buy" else -1
            pnl += (price - p["entry"]) * p["amount"] * direction
        return self.balance + pnl

    async def fetch_balance(self):
        return {"USDT": {"free": self.free, "used": self.used, "total": self.balance}}

    def amount_to_precision(self, symbol, amount):
        return round(amount, 4)

    async def create_order(self, symbol, type_, side, amount, params=None):
        params = params or {}
        price = self.prices.get(symbol)
        if price is None or price <= 0:
            raise ValueError("No price")
        amount = float(amount)
        notional = amount * price
        fee = notional * self.fee_rate
        closing = params.get("reduceOnly", False)
        scale_in = params.get("scaleIn", False)

        if closing:
            pos = self.positions.get(symbol)
            if not pos:
                raise ValueError("No position to close")
            direction = 1 if pos["side"] == "buy" else -1
            pnl = (price - pos["entry"]) * amount * direction
            self.balance += pnl - fee
            del self.positions[symbol]
            trade = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol, "side": side, "amount": amount,
                "price": price, "pnl_usdt": pnl, "fee_usdt": fee,
                "reduceOnly": True
            }
            self.trades.append(trade)
            return trade

        if symbol in self.positions and not scale_in:
            raise ValueError("Position already exists")

        margin = notional / LEVERAGE
        if margin + fee > self.free and not scale_in:
            raise ValueError("Insufficient margin")

        self.balance -= fee

        if scale_in and symbol in self.positions:
            old = self.positions[symbol]
            total_amount = old["amount"] + amount
            avg_entry = (old["entry"] * old["amount"] + price * amount) / total_amount
            self.positions[symbol] = {
                "side": old["side"],
                "entry": avg_entry,
                "amount": total_amount,
                "margin": old["margin"] + margin,
                "original_entry": old.get("original_entry", old["entry"]),
                "original_notional": old.get("original_notional", old["entry"] * old["amount"]),
            }
        else:
            self.positions[symbol] = {
                "side": side,
                "entry": price,
                "amount": amount,
                "margin": margin,
                "original_entry": price,
                "original_notional": notional,
            }

        trade = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol, "side": side, "amount": amount,
            "price": price, "pnl_usdt": 0.0, "fee_usdt": fee,
            "reduceOnly": False, "scaleIn": scale_in
        }
        self.trades.append(trade)
        return trade

# ====================== BOT ======================
class Final3Pro:
    def __init__(self):
        self.exchange = ccxt.binanceusdm({
            "enableRateLimit": True,
            "options": {"defaultType": "future"}
        })
        self.account = PaperAccount()
        self.positions = {}
        self.last_trade = {}
        self.lock = asyncio.Lock()
        self.load_state()

    def load_state(self):
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                self.account.balance = data.get("balance", 100.0)
                self.account.positions = data.get("account_positions", {})
                self.account.trades = data.get("trades", [])
                self.positions = data.get("strategy_positions", {})
                self.last_trade = data.get("last_trade", {})
                log.info(f"State loaded | balance={self.account.balance:.2f}")
            except Exception as e:
                log.warning(f"State load failed: {e}")

    def save_state(self):
        data = {
            "balance": self.account.balance,
            "equity": self.account.equity,
            "free": self.account.free,
            "used": self.account.used,
            "account_positions": self.account.positions,
            "strategy_positions": self.positions,
            "trades": self.account.trades[-100:],
            "last_trade": self.last_trade,
            "updated": datetime.now(timezone.utc).isoformat()
        }
        STATE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    async def get_top_symbols(self):
        try:
            tickers = await self.exchange.fetch_tickers()
            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith(":USDT") or "/USDT:USDT" not in symbol:
                    continue
                vol = float(t.get("quoteVolume") or 0)
                if vol >= MIN_QUOTE_VOLUME:
                    candidates.append((symbol, vol))
            candidates.sort(key=lambda x: x[1], reverse=True)
            return [s for s, _ in candidates[:MAX_SYMBOLS]]
        except Exception as e:
            log.error(f"Top symbols error: {e}")
            return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    async def get_imbalance(self, symbol):
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=20)
            bid = sum(x[1] for x in ob["bids"][:20])
            ask = sum(x[1] for x in ob["asks"][:20])
            return (bid - ask) / (bid + ask) if (bid + ask) > 0 else 0.0
        except:
            return 0.0

    async def detect(self, symbol):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, "1m", limit=30)
            df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
            ret = (df["c"].iloc[-1] - df["c"].iloc[-3]) / df["c"].iloc[-3]
            vol_avg = df["v"].iloc[-15:-3].mean()
            vol_spike = df["v"].iloc[-1] / vol_avg if vol_avg > 0 else 0
            imb = await self.get_imbalance(symbol)

            # light 5m bias
            try:
                ohlcv5 = await self.exchange.fetch_ohlcv(symbol, "5m", limit=8)
                df5 = pd.DataFrame(ohlcv5, columns=["ts", "o", "h", "l", "c", "v"])
                mtf = (df5["c"].iloc[-1] - df5["c"].iloc[-3]) / df5["c"].iloc[-3]
            except:
                mtf = 0.0

            long_sig = ret > CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb > 0.35
            short_sig = ret < -CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb < -0.35

            strength = abs(ret) * vol_spike * abs(imb)
            if long_sig and mtf > 0:
                strength *= 1.12
            elif short_sig and mtf < 0:
                strength *= 1.12

            return {
                "long": long_sig, "short": short_sig,
                "ret": ret, "vol_spike": vol_spike, "imb": imb,
                "strength": strength, "price": float(df["c"].iloc[-1]),
                "symbol": symbol
            }
        except Exception as e:
            return None

    async def size_position(self, symbol, strength):
        bal = self.account.balance
        ticker = await self.exchange.fetch_ticker(symbol)
        price = ticker["last"]
        self.account.prices[symbol] = price

        margin_pct = BASE_MARGIN_PCT
        if strength > 0.09:
            margin_pct = min(MAX_MARGIN_PCT, BASE_MARGIN_PCT * 1.55)

        notional = bal * margin_pct * LEVERAGE
        amount = notional / price
        amount = float(self.exchange.amount_to_precision(symbol, amount))
        return amount, margin_pct, price

    async def enter(self, symbol, side, price, strength):
        now = time.time()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            return False
        if len(self.positions) >= MAX_POSITIONS:
            return False

        amount, margin_pct, fill_price = await self.size_position(symbol, strength)
        if amount <= 0:
            return False

        try:
            await self.account.create_order(symbol, "market", side, amount)
            self.last_trade[symbol] = now
            self.positions[symbol] = {
                "side": side,
                "entry": fill_price,
                "amount": amount,
                "sl": fill_price * (1 - STOP_LOSS_PCT) if side == "buy" else fill_price * (1 + STOP_LOSS_PCT),
                "tp": fill_price * (1 + TAKE_PROFIT_PCT) if side == "buy" else fill_price * (1 - TAKE_PROFIT_PCT),
                "original_entry": fill_price,
                "original_notional": fill_price * amount,
                "adds_done": 0,
                "trailing": False,
                "best_price": fill_price,
                "strength": strength
            }
            log.info(f"ENTER {side.upper()} {symbol} | size={amount:.4f} @ {fill_price:.4f} | strength={strength:.3f} | margin={margin_pct*100:.1f}%")
            self.save_state()
            return True
        except Exception as e:
            log.error(f"Enter failed {symbol}: {e}")
            return False

    async def add_position(self, symbol, price):
        p = self.positions[symbol]
        amount = p["original_notional"] / price
        amount = float(self.exchange.amount_to_precision(symbol, amount))
        try:
            await self.account.create_order(symbol, "market", p["side"], amount, params={"scaleIn": True})
            fill = self.account.positions[symbol]
            p["entry"] = fill["entry"]
            p["amount"] = fill["amount"]
            p["adds_done"] = 1
            p["tp"] = p["entry"] * (1 + TAKE_PROFIT_PCT if p["side"] == "buy" else 1 - TAKE_PROFIT_PCT)
            log.info(f"SCALE-IN {symbol} | new avg={p['entry']:.4f}")
            self.save_state()
            return True
        except Exception as e:
            log.warning(f"Scale-in failed {symbol}: {e}")
            return False

    async def manage_positions(self):
        for symbol in list(self.positions.keys()):
            try:
                ticker = await self.exchange.fetch_ticker(symbol)
                price = ticker["last"]
                self.account.prices[symbol] = price
                p = self.positions[symbol]
                direction = 1 if p["side"] == "buy" else -1

                # Stop loss priority
                if direction * (price - p["sl"]) <= 0:
                    await self.close(symbol, "SL" if not p["trailing"] else "TRAIL_SL", price)
                    continue

                # Scale-in
                adverse = -direction * (price / p["original_entry"] - 1)
                if p["adds_done"] == 0 and adverse >= ADD_LEVEL:
                    await self.add_position(symbol, price)

                # Trailing activation
                gain = direction * (price / p["entry"] - 1)
                if not p["trailing"] and gain >= TAKE_PROFIT_PCT:
                    p["trailing"] = True
                    p["best_price"] = price

                if p["trailing"]:
                    if direction == 1:
                        p["best_price"] = max(p["best_price"], price)
                        new_sl = p["best_price"] * (1 - 0.009)
                        p["sl"] = max(p["sl"], new_sl)
                    else:
                        p["best_price"] = min(p["best_price"], price)
                        new_sl = p["best_price"] * (1 + 0.009)
                        p["sl"] = min(p["sl"], new_sl)

                self.save_state()
            except Exception as e:
                log.warning(f"Manage error {symbol}: {e}")

    async def close(self, symbol, reason, price):
        p = self.positions[symbol]
        side = "sell" if p["side"] == "buy" else "buy"
        try:
            await self.account.create_order(symbol, "market", side, p["amount"], params={"reduceOnly": True})
            log.info(f"EXIT {reason} {symbol} @ {price:.4f}")
            del self.positions[symbol]
            self.save_state()
        except Exception as e:
            log.error(f"Close failed {symbol}: {e}")

    async def scan_cycle(self):
        symbols = await self.get_top_symbols()
        log.info(f"Scanning {len(symbols)} symbols...")
        signals = []
        for sym in symbols:
            sig = await self.detect(sym)
            if sig and (sig["long"] or sig["short"]):
                signals.append(sig)
            await asyncio.sleep(0.15)

        signals.sort(key=lambda x: x["strength"], reverse=True)
        async with self.lock:
            await self.manage_positions()
            for sig in signals:
                if len(self.positions) >= MAX_POSITIONS:
                    break
                if sig["symbol"] in self.positions:
                    continue
                side = "buy" if sig["long"] else "sell"
                await self.enter(sig["symbol"], side, sig["price"], sig["strength"])

    async def status_printer(self):
        while True:
            await asyncio.sleep(30)
            eq = self.account.equity
            log.info(f"STATUS | balance={self.account.balance:.2f} | equity={eq:.2f} | free={self.account.free:.2f} | positions={len(self.positions)}")

    async def run(self):
        log.info("Final-3 Pro started | 7% SL | dynamic size | light MTF")
        asyncio.create_task(self.status_printer())
        while True:
            try:
                await self.scan_cycle()
            except Exception as e:
                log.error(f"Cycle error: {e}")
            log.info(f"Next scan in {SCAN_INTERVAL}s")
            await asyncio.sleep(SCAN_INTERVAL)

async def main():
    bot = Final3Pro()
    try:
        await bot.run()
    except KeyboardInterrupt:
        bot.save_state()
        log.info("Stopped & saved")

if __name__ == "__main__":
    asyncio.run(main())
