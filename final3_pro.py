# language: Python 3.11+
# file: final3_pro.py
# Final-3 Pro — Single File | Dynamic size + light MTF + 7% SL + 1 scale-in

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
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
MAX_PRICE_AGE = 12.0
TICKER_TIMEOUT = 8.0
EXIT_INTERVAL = 8.0
TRAIL_DISTANCE = 0.009
STATE_FILE = Path("paper_state_final3_pro.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("FINAL3-PRO")

def validated_price(ticker):
    timestamp = ticker.get("timestamp")
    if timestamp is None:
        raise ValueError("Missing ticker timestamp; operation rejected")
    age = time.time() - float(timestamp) / 1000
    if not math.isfinite(age) or abs(age) > MAX_PRICE_AGE:
        raise ValueError(f"Stale ticker ({age:.1f}s); operation rejected")
    price = float(ticker["last"])
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Invalid ticker price")
    return price


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

    async def create_order(self, symbol, type_, side, amount, params=None, *, ticker=None):
        params = params or {}
        price = validated_price(ticker or {})
        amount = float(amount)
        if type_ != "market" or side not in {"buy", "sell"}:
            raise ValueError("Only buy/sell market orders are supported")
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Invalid amount")
        notional = amount * price
        fee = notional * self.fee_rate
        closing = params.get("reduceOnly", False)
        scale_in = params.get("scaleIn", False)

        if closing:
            pos = self.positions.get(symbol)
            if (not pos or side == pos["side"]
                    or not math.isclose(amount, pos["amount"], rel_tol=1e-9)):
                raise ValueError("Invalid full-position close")
            direction = 1 if pos["side"] == "buy" else -1
            pnl = (price - pos["entry"]) * amount * direction
            self.prices[symbol] = price
            self.balance += pnl - fee
            del self.positions[symbol]
            trade = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol, "side": side, "amount": amount,
                "price": price, "pnl_usdt": pnl, "fee_usdt": fee,
                "reduceOnly": True,
                "exit_reason": params.get("exit_reason"),
                "market_timestamp": ticker["timestamp"]
            }
            self.trades.append(trade)
            return trade

        if symbol in self.positions and not scale_in:
            raise ValueError("Position already exists")

        if scale_in and (symbol not in self.positions or self.positions[symbol]["side"] != side):
            raise ValueError("Scale-in must match an existing position")

        margin = notional / LEVERAGE
        if margin + fee > self.free:
            raise ValueError("Insufficient margin")

        self.prices[symbol] = price
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
            "reduceOnly": False, "scaleIn": scale_in,
            "market_timestamp": ticker["timestamp"],
            "strength": params.get("strength"),
            "margin_pct": params.get("margin_pct"),
            "margin_usdt": margin
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
                data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
                self.account.balance = data.get("balance", 100.0)
                self.account.positions = data.get("account_positions", {})
                self.account.trades = data.get("trades", [])
                self.positions = data.get("strategy_positions", {})
                self.last_trade = data.get("last_trade", {})
                self.account.prices = data.get("prices", {})
                if set(self.positions) != set(self.account.positions):
                    raise ValueError("Inconsistent saved positions")
                for symbol, pos in self.positions.items():
                    fill = self.account.positions[symbol]
                    pos.setdefault("original_entry", fill.get("original_entry", pos["entry"]))
                    pos.setdefault("original_notional", fill.get("original_notional", pos["entry"] * pos["amount"]))
                    pos.setdefault("adds_done", 0)
                    pos.setdefault("trailing", False)
                    pos.setdefault("best_price", pos["entry"])
                log.info(f"State loaded | balance={self.account.balance:.2f}")
            except Exception as e:
                raise RuntimeError(f"State load failed; original file preserved: {e}") from e

    def save_state(self):
        data = {
            "balance": self.account.balance,
            "equity": self.account.equity,
            "free": self.account.free,
            "used": self.account.used,
            "account_positions": self.account.positions,
            "strategy_positions": self.positions,
            "trades": self.account.trades[-150:],
            "prices": self.account.prices,
            "last_trade": self.last_trade,
            "updated": datetime.now(timezone.utc).isoformat()
        }
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        temporary.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
        temporary.replace(STATE_FILE)

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

    async def fresh_ticker(self, symbol):
        ticker = await asyncio.wait_for(self.exchange.fetch_ticker(symbol), TICKER_TIMEOUT)
        price = validated_price(ticker)
        self.account.prices[symbol] = price
        return ticker

    @staticmethod
    def margin_fraction(strength):
        if not math.isfinite(strength):
            raise ValueError("Invalid signal strength")
        # Linear growth: strength 0.09 -> 10%, strength 0.18 -> 17%.
        progress = min(1.0, max(0.0, (strength - 0.09) / 0.09))
        return BASE_MARGIN_PCT + (MAX_MARGIN_PCT - BASE_MARGIN_PCT) * progress

    async def size_position(self, symbol, strength, *, ticker=None):
        ticker = ticker if ticker is not None else await self.fresh_ticker(symbol)
        price = validated_price(ticker)
        margin_pct = self.margin_fraction(strength)
        notional = max(0.0, self.account.balance) * margin_pct * LEVERAGE
        amount = float(self.exchange.amount_to_precision(symbol, notional / price))
        return amount, margin_pct, price

    async def enter(self, symbol, side, price, strength):
        now = time.time()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            return False
        if symbol in self.positions or len(self.positions) >= MAX_POSITIONS:
            return False

        try:
            ticker = await self.fresh_ticker(symbol)
            amount, margin_pct, fill_price = await self.size_position(symbol, strength, ticker=ticker)
            if amount <= 0:
                return False
            await self.account.create_order(
                symbol, "market", side, amount,
                params={"strength": strength, "margin_pct": margin_pct}, ticker=ticker)
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
                "strength": strength,
                "margin_pct": margin_pct
            }
            log.info(f"ENTER {side.upper()} {symbol} | size={amount:.4f} @ {fill_price:.4f} | strength={strength:.3f} | margin={margin_pct*100:.1f}%")
            self.save_state()
            return True
        except Exception as e:
            log.error(f"Enter failed {symbol}: {e}")
            return False

    async def add_position(self, symbol, price=None, *, ticker=None):
        try:
            p = self.positions[symbol]
            if p["adds_done"] >= 1 or p["trailing"]:
                return False
            ticker = ticker if ticker is not None else await self.fresh_ticker(symbol)
            price = validated_price(ticker)
            direction = 1 if p["side"] == "buy" else -1
            if direction * (price - p["sl"]) <= 0:
                return False
            if -direction * (price / p["original_entry"] - 1) + 1e-12 < ADD_LEVEL:
                return False
            amount = float(self.exchange.amount_to_precision(symbol, p["original_notional"] / price))
            required = amount * price * (1 / LEVERAGE + self.account.fee_rate)
            if amount <= 0 or required > self.account.free:
                log.warning("Scale-in rejected %s: insufficient free margin including fee", symbol)
                return False
            await self.account.create_order(
                symbol, "market", p["side"], amount,
                params={"scaleIn": True, "strength": p.get("strength"),
                        "margin_pct": p.get("margin_pct")}, ticker=ticker)
            fill = self.account.positions[symbol]
            p.update(entry=fill["entry"], amount=fill["amount"], adds_done=1)
            p["tp"] = p["entry"] * (1 + direction * TAKE_PROFIT_PCT)
            log.info("SCALE-IN %s | new avg=%.4f", symbol, p["entry"])
            self.save_state()
            return True
        except Exception as e:
            log.warning("Scale-in failed %s: %s", symbol, e)
            return False

    async def manage_position(self, symbol):
        try:
            # Fetch concurrently; serialize account mutations with entries.
            ticker = await self.fresh_ticker(symbol)
            async with self.lock:
                if symbol not in self.positions:
                    return
                price = validated_price(ticker)
                p = self.positions[symbol]
                direction = 1 if p["side"] == "buy" else -1
                if direction * (price - p["sl"]) <= 0:
                    await self.close(symbol, "TRAIL_SL" if p["trailing"] else "SL", price, ticker=ticker)
                    return
                adverse = -direction * (price / p["original_entry"] - 1)
                if not p["trailing"] and p["adds_done"] == 0 and adverse + 1e-12 >= ADD_LEVEL:
                    await self.add_position(symbol, price, ticker=ticker)
                gain = direction * (price / p["entry"] - 1)
                if not p["trailing"] and gain + 1e-12 >= TAKE_PROFIT_PCT:
                    p["trailing"] = True
                    p["best_price"] = price
                if p["trailing"]:
                    if direction == 1:
                        p["best_price"] = max(p["best_price"], price)
                        p["sl"] = max(p["sl"], p["best_price"] * (1 - TRAIL_DISTANCE))
                    else:
                        p["best_price"] = min(p["best_price"], price)
                        p["sl"] = min(p["sl"], p["best_price"] * (1 + TRAIL_DISTANCE))
                self.save_state()
        except Exception as e:
            log.warning("Manage error %s: %s", symbol, e)

    async def manage_positions(self):
        await asyncio.gather(*(self.manage_position(s) for s in list(self.positions)))

    async def close(self, symbol, reason, price=None, *, ticker=None):
        try:
            p = self.positions[symbol]
            ticker = ticker if ticker is not None else await self.fresh_ticker(symbol)
            price = validated_price(ticker)
            side = "sell" if p["side"] == "buy" else "buy"
            await self.account.create_order(
                symbol, "market", side, p["amount"],
                params={"reduceOnly": True, "exit_reason": reason}, ticker=ticker)
            del self.positions[symbol]
            self.save_state()
            log.info("EXIT %s %s @ %.4f", reason, symbol, price)
            return True
        except Exception as e:
            log.error("Close failed %s: %s", symbol, e)
            return False

    async def exit_watcher(self):
        while True:
            started = time.monotonic()
            try:
                await self.manage_positions()
            except Exception as e:
                log.warning("Exit watcher error: %s", e)
            await asyncio.sleep(max(0.0, EXIT_INTERVAL - (time.monotonic() - started)))

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
        for sig in signals:
            async with self.lock:
                if len(self.positions) >= MAX_POSITIONS:
                    break
                if sig["symbol"] in self.positions:
                    continue
                side = "buy" if sig["long"] else "sell"
                await self.enter(sig["symbol"], side, sig["price"], sig["strength"])

    async def status_printer(self):
        while True:
            await asyncio.sleep(30)
            recent = []
            for trade in self.account.trades[-3:]:
                action = trade.get("exit_reason", "CLOSE") if trade.get("reduceOnly") else (
                    "SCALE_IN" if trade.get("scaleIn") else "ENTRY")
                recent.append(f"{action} {trade['symbol']} {trade['side']} "
                              f"@ {trade['price']:.6g} pnl={trade.get('pnl_usdt', 0):+.2f} "
                              f"fee={trade.get('fee_usdt', 0):.4f}")
            log.info("STATUS | balance=%.2f | equity=%.2f | free=%.2f | positions=%d | recent=%s",
                     self.account.balance, self.account.equity, self.account.free,
                     len(self.positions), "; ".join(recent) or "none")

    async def scan_loop(self):
        while True:
            try:
                await self.scan_cycle()
            except Exception as e:
                log.error("Cycle error: %s", e)
            log.info("Next scan in %ss", SCAN_INTERVAL)
            await asyncio.sleep(SCAN_INTERVAL)

    async def run(self):
        log.info("Final-3 Pro started | 7% SL | dynamic size 10-17% | light MTF")
        tasks = []
        try:
            await self.exchange.load_markets()
            tasks = [asyncio.create_task(self.exit_watcher()),
                     asyncio.create_task(self.status_printer()),
                     asyncio.create_task(self.scan_loop())]
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                self.save_state()
            finally:
                await self.exchange.close()
            log.info("Stopped & saved")


async def main():
    bot = Final3Pro()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
