# language: Python 3.11+
# file: final3_pro.py
# Final-3 Pro — Single File | Dynamic size + MTF + post-add protection

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
import logging
from collections import deque
from types import SimpleNamespace
from urllib.parse import urlencode

import aiohttp

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
POST_ADD_STOP_PCT = 0.04
PARTIAL_ADVERSE_PCT = 0.03
PARTIAL_CLOSE_FRACTION = 0.50
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


WS_URL = "wss://fstream.binance.com/market/stream"
MAX_EVENT_AGE = 10.0
BOOK_CHECK_INTERVAL = 1.0
BOOK_REQUEST_SPACING = 0.25
WORKERS = 8
FRESHNESS_TIMEOUT = 15.0
WATCHDOG_INTERVAL = 2.0
SEED_TIMEOUT = 30.0


class CandleGap(Exception):
    pass


def valid_rows(rows):
    cleaned = []
    for row in rows:
        if len(row) < 6:
            raise ValueError("Incomplete candle")
        t = int(row[0])
        o, h, l, c, v = map(float, row[1:6])
        if (t % 60000 or not all(math.isfinite(x) for x in (o, h, l, c, v))
                or min(o, h, l, c) <= 0 or v < 0
                or l > min(o, c) or h < max(o, c)):
            raise ValueError("Invalid candle")
        cleaned.append([t, o, h, l, c, v])
    if len(cleaned) < 15 or any(b[0] - a[0] != 60000 for a, b in zip(cleaned, cleaned[1:])):
        raise ValueError("Insufficient or gapped candle history")
    return cleaned[-30:]


class ContinuousEntryScanner:
    def __init__(self, bot, *, config=None, clock=time.monotonic, wall_clock=time.time):
        self.bot = bot
        self.config = config or SimpleNamespace(
            MAX_SYMBOLS_TO_SCAN=MAX_SYMBOLS, MAX_CONCURRENT_POSITIONS=MAX_POSITIONS,
            CASCADE_THRESHOLD=CASCADE_THRESHOLD, VOLUME_SPIKE=VOLUME_SPIKE,
            SCAN_INTERVAL=SCAN_INTERVAL, log=log)
        self.clock = clock
        self.wall_clock = wall_clock
        self.symbols = ()
        self.histories = {}
        self.stream_ids = {}
        self.event_times = {}
        self.received = {}
        self.last_book_check = {}
        self.queue = asyncio.Queue(maxsize=self.config.MAX_SYMBOLS_TO_SCAN)
        self.pending = set()
        self.changed = asyncio.Event()
        self.ready = asyncio.Event()
        self.connected = False
        self.book_lock = asyncio.Lock()
        self.next_book_request = 0.0
        self.book_pause_until = 0.0
        self.events = self.book_checks = self.entries = 0
        self.signal_checks = 0
        self.entry_results = {}
        self.last_error = None
        self.checks = {}
        self.last_universe_at = self.next_universe_at = None

    async def refresh_universe(self):
        symbols = await self.bot.get_top_symbols()
        accepted = []
        for symbol in symbols:
            try:
                market = self.bot.exchange.market(symbol)
                if not (market.get("linear") and market.get("swap")
                        and market.get("settle") == "USDT" and market.get("active") is not False):
                    continue
                accepted.append(symbol)
                self.stream_ids[symbol] = market.get("id") or symbol.split(":")[0].replace("/", "")
            except Exception as error:
                self.config.log.warning("Skipping market %s: %s", symbol, error)
        selected = tuple(accepted[:self.config.MAX_SYMBOLS_TO_SCAN])
        if selected != self.symbols:
            self.symbols = selected
            self.changed.set()
        self.last_universe_at = self.wall_clock()
        self.next_universe_at = self.last_universe_at + self.config.SCAN_INTERVAL
        self.checks = {s:r for s,r in self.checks.items() if s in selected}
        self.ready.set()
        self.config.log.info("CONTINUOUS UNIVERSE | symbols=%d | refresh=%ss", len(selected), self.config.SCAN_INTERVAL)

    async def universe_loop(self):
        while True:
            try:
                await self.refresh_universe()
            except Exception as error:
                self.last_error = f"Universe refresh: {error}"
                self.config.log.warning("Universe refresh failed: %s", error)
                await asyncio.sleep(10)
                continue
            await asyncio.sleep(self.config.SCAN_INTERVAL)

    async def seed(self, symbols):
        semaphore = asyncio.Semaphore(WORKERS)

        async def one(symbol):
            async with semaphore:
                try:
                    rows = await asyncio.wait_for(
                        self.bot.exchange.fetch_ohlcv(symbol, "1m", limit=30), SEED_TIMEOUT)
                    self.histories[symbol] = deque(valid_rows(rows), maxlen=30)
                    return symbol
                except Exception as error:
                    self.checks[symbol] = dict(symbol=symbol, status='error', checked_at=self.wall_clock(),
                        errors=[dict(stage='warmup', message=str(error))])
                    self.config.log.warning("Candle warmup failed %s: %s", symbol, error)
                    return None

        return [s for s in await asyncio.gather(*(one(s) for s in symbols)) if s is not None]

    def schedule(self, symbol):
        if symbol not in self.pending and symbol in self.symbols:
            try:
                self.queue.put_nowait(symbol)
            except asyncio.QueueFull:
                return
            self.pending.add(symbol)

    def ingest(self, message):
        payload = message.get("data", message)
        if payload.get("e") != "kline" or payload.get("st", 1) != 1:
            return
        kline = payload["k"]
        if kline.get("i") != "1m":
            return
        stream_id = kline.get("s", payload.get("s"))
        symbol = next((s for s in self.symbols if self.stream_ids.get(s) == stream_id), None)
        if symbol is None or symbol not in self.histories:
            return
        event = int(payload["E"])
        if event <= self.event_times.get(symbol, -1):
            return
        age = self.wall_clock() - event / 1000
        if age > MAX_EVENT_AGE or age < -MAX_EVENT_AGE:
            return
        row = [int(kline["t"]), *map(float, (kline["o"], kline["h"], kline["l"], kline["c"], kline["v"]))]
        history = self.histories[symbol]
        if row[0] < history[-1][0]:
            return
        if row[0] > history[-1][0] + 60000:
            raise CandleGap(symbol)
        valid_rows([*list(history)[:-1], row] if row[0] == history[-1][0] else [*history, row])
        if row[0] == history[-1][0]:
            history[-1] = row
        else:
            history.append(row)
        self.event_times[symbol] = event
        self.received[symbol] = self.clock()
        self.events += 1
        metrics = self.metrics(symbol)
        if metrics is not None:
            previous = self.checks.get(symbol, {})
            book_recent = self.clock() - self.last_book_check.get(symbol, -math.inf) <= BOOK_CHECK_INTERVAL
            self.checks[symbol] = dict(symbol=symbol, status='watched', checked_at=self.wall_clock(),
                ret=metrics['ret'], vol_spike=metrics['spike'], mtf=metrics['mtf'],
                move_pass=bool(abs(metrics['ret']) > self.config.CASCADE_THRESHOLD),
                volume_pass=bool(metrics['spike'] > self.config.VOLUME_SPIKE),
                imb=previous.get('imb') if book_recent else None,
                book_pass=previous.get('book_pass') if book_recent else None,
                signal=None, errors=[])
        self.schedule(symbol)

    def metrics(self, symbol):
        rows = list(self.histories.get(symbol, ()))
        if len(rows) < 15:
            return None
        ret = (rows[-1][4] - rows[-3][4]) / rows[-3][4]
        average = sum(r[5] for r in rows[-15:-3]) / 12
        spike = rows[-1][5] / average if average > 0 else 0.0
        # Last closes of 5-minute buckets reproduce the original light MTF return.
        buckets = {}
        for row in rows:
            buckets[row[0] // 300000] = row[4]
        closes = list(buckets.values())
        mtf = (closes[-1]-closes[-3])/closes[-3] if len(closes) >= 3 else 0.0
        return dict(side='buy' if ret > 0 else 'sell', price=rows[-1][4], ret=ret, spike=spike, mtf=mtf)

    def signal(self, symbol):
        if (not self.connected or symbol not in self.symbols or symbol not in self.received
                or self.clock()-self.received[symbol] > MAX_EVENT_AGE
                or abs(self.wall_clock()-self.event_times[symbol]/1000) > MAX_EVENT_AGE):
            return None
        metrics = self.metrics(symbol)
        if (metrics is None or abs(metrics['ret']) <= self.config.CASCADE_THRESHOLD
                or metrics['spike'] <= self.config.VOLUME_SPIKE):
            return None
        return metrics

    async def evaluate(self, symbol):
        if symbol in self.bot.positions or len(self.bot.positions) >= self.config.MAX_CONCURRENT_POSITIONS:
            return
        signal = self.signal(symbol)
        now = self.clock()
        if (signal is None or now < self.book_pause_until
                or now-self.last_book_check.get(symbol, -math.inf) < BOOK_CHECK_INTERVAL):
            return
        self.last_book_check[symbol] = now
        async with self.book_lock:
            await asyncio.sleep(max(0.0, self.next_book_request-self.clock()))
            if self.clock() < self.book_pause_until or self.signal(symbol) is None:
                return
            self.next_book_request = self.clock()+BOOK_REQUEST_SPACING
        try:
            self.book_checks += 1
            book = await asyncio.wait_for(self.bot.exchange.fetch_order_book(symbol, limit=20), TICKER_TIMEOUT)
            bids = sum(float(x[1]) for x in book['bids'][:20])
            asks = sum(float(x[1]) for x in book['asks'][:20])
            if not all(math.isfinite(v) and v >= 0 for v in (bids, asks)):
                raise ValueError('Invalid order book quantities')
            imbalance = (bids-asks)/(bids+asks) if bids+asks else 0.0
        except Exception as error:
            self.book_pause_until = self.clock()+5
            self.last_error = f"Order book {symbol}: {error}"
            self.checks.setdefault(symbol, {'symbol':symbol}).update(status='error',
                errors=[dict(stage='order_book',message=str(error))])
            self.config.log.warning('Order book check failed %s (backoff 5s): %s', symbol, error)
            return
        async with self.bot.lock:
            signal = self.signal(symbol)
            if (signal is None or symbol in self.bot.positions
                    or len(self.bot.positions) >= self.config.MAX_CONCURRENT_POSITIONS):
                return
            book_pass = bool((signal['side']=='buy' and imbalance > 0.35)
                             or (signal['side']=='sell' and imbalance < -0.35))
            row = self.checks.setdefault(symbol, {'symbol':symbol})
            row.update(status='watched', checked_at=self.wall_clock(), imb=imbalance, book_pass=book_pass, errors=[])
            if not book_pass:
                return
            strength = abs(signal['ret'])*signal['spike']*abs(imbalance)
            if (signal['side']=='buy' and signal['mtf']>0) or (signal['side']=='sell' and signal['mtf']<0):
                strength *= 1.12
            row.update(strength=strength, signal='LONG' if signal['side']=='buy' else 'SHORT')
            self.signal_checks += 1
            def guard():
                current = self.signal(symbol)
                return current is not None and current['side'] == signal['side']
            if await self.bot.enter(symbol,signal['side'],signal['price'],strength,signal_guard=guard):
                self.entries += 1
                trade = self.bot.account.trades[-1]
                trade.update(entry_reason='CASCADE_MOVE_VOLUME_ORDERBOOK', entry_signal={
                    **signal, 'imbalance':imbalance, 'strength':strength,
                    'market_event_time':self.event_times[symbol]})
                self.bot.save_state()
                self.config.log.info('CONTINUOUS ENTRY %s | strength=%.4f',symbol,strength)
            self.entry_results[symbol] = self.bot.entry_outcomes.get(symbol,'REJECTED')

    def diagnostics(self):
        rows = list(self.checks.values())
        return dict(mode='continuous', phase='watching' if self.connected else 'reconnecting',
            total=len(self.symbols), checked=sum(s in self.received for s in self.symbols),
            successful=sum(x.get('status')!='error' for x in rows),
            failed=sum(x.get('status')=='error' for x in rows), signals=self.signal_checks, entries=self.entries,
            move_pass=sum(x.get('move_pass') is True for x in rows),
            volume_pass=sum(x.get('volume_pass') is True for x in rows),
            book_pass=sum(x.get('book_pass') is True for x in rows),
            checks=self.checks, entry_results=self.entry_results,
            last_universe_at=self.last_universe_at, next_universe_at=self.next_universe_at,
            events=self.events, book_checks=self.book_checks, last_error=self.last_error)

    async def worker(self):
        while True:
            symbol = await self.queue.get()
            try:
                await self.evaluate(symbol)
            except Exception as error:
                self.config.log.warning("Continuous check failed %s: %s", symbol, error)
            finally:
                self.pending.discard(symbol)
                self.queue.task_done()

    async def read_stream(self, socket):
        async for message in socket:
            if message.type == aiohttp.WSMsgType.TEXT:
                payload = message.json()
                if "code" in payload:
                    raise ConnectionError(str(payload))
                self.ingest(payload)
            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE,
                                  aiohttp.WSMsgType.CLOSED):
                break
        raise ConnectionError("Candle stream closed")

    def stale_symbols(self, symbols, started):
        now = self.clock()
        stale = []
        for symbol in symbols:
            received = self.received.get(symbol)
            if received is None:
                expired = now - started > FRESHNESS_TIMEOUT
            else:
                age = self.wall_clock() - self.event_times[symbol] / 1000
                expired = (now - received > FRESHNESS_TIMEOUT
                           or age > FRESHNESS_TIMEOUT or age < -MAX_EVENT_AGE)
            if expired:
                stale.append(symbol)
        return stale

    async def freshness_watchdog(self, symbols):
        # Per-symbol accepted updates, not socket activity or heartbeat frames.
        started = self.clock()
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            stale = self.stale_symbols(symbols, started)
            if stale and (len(stale) == len(symbols) or self.clock()-started > 60):
                self.connected = False  # Block new signals before reconnect cleanup.
                raise ConnectionError("No fresh accepted candles: " + ", ".join(stale))

    async def stream_loop(self):
        await self.ready.wait()
        delay = 1
        timeout = aiohttp.ClientTimeout(total=None, connect=20)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            while True:
                self.changed.clear()
                self.connected = False
                self.received.clear()
                self.event_times.clear()
                symbols = tuple(self.symbols)
                try:
                    if not symbols:
                        await asyncio.sleep(5)
                        continue
                    warmed = await self.seed(symbols)
                    if not warmed:
                        raise ConnectionError("No usable candle history")
                    if self.changed.is_set():
                        continue
                    streams = "/".join(self.stream_ids[s].lower() + "@kline_1m" for s in warmed)
                    url = WS_URL + "?" + urlencode({"streams": streams})
                    async with session.ws_connect(url, heartbeat=20, receive_timeout=30) as socket:
                        self.connected = True
                        self.last_error = None
                        self.config.log.info("CONTINUOUS WATCH CONNECTED | symbols=%d | entries checked on updates", len(warmed))
                        delay = 1
                        reader = asyncio.create_task(self.read_stream(socket))
                        watchdog = asyncio.create_task(self.freshness_watchdog(tuple(warmed)))
                        change = asyncio.create_task(self.changed.wait())
                        reseed = asyncio.create_task(asyncio.sleep(30 if len(warmed) < len(symbols) else 23 * 3600))
                        try:
                            done, _ = await asyncio.wait((reader, change, reseed, watchdog), return_when=asyncio.FIRST_COMPLETED)
                            if watchdog in done:
                                await watchdog
                            if reader in done:
                                await reader
                        finally:
                            self.connected = False
                            watchdog.cancel()
                            reader.cancel()
                            change.cancel()
                            reseed.cancel()
                            await asyncio.gather(reader, change, reseed, watchdog, return_exceptions=True)
                except Exception as error:
                    self.connected = False
                    self.last_error = str(error)
                    self.config.log.warning("Candle stream reconnect in %ss: %s", delay, error)
                    await asyncio.sleep(delay)
                    delay = min(30, delay * 2)

    async def status_loop(self):
        while True:
            await asyncio.sleep(30)
            self.config.log.info("CONTINUOUS STATUS | connected=%s symbols=%d events=%d book_checks=%d entries=%d",
                            self.connected, len(self.symbols), self.events, self.book_checks, self.entries)

    async def run(self):
        tasks = [asyncio.create_task(self.universe_loop()), asyncio.create_task(self.stream_loop()),
                 asyncio.create_task(self.status_loop())]
        tasks.extend(asyncio.create_task(self.worker()) for _ in range(WORKERS))
        try:
            await asyncio.gather(*tasks)
        finally:
            self.connected = False
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


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
            if not pos or side == pos["side"] or amount > pos["amount"]:
                raise ValueError("Invalid reduce-only amount or direction")
            full_close = amount == pos["amount"]
            direction = 1 if pos["side"] == "buy" else -1
            pnl = (price - pos["entry"]) * amount * direction
            released_margin = pos["margin"] * amount / pos["amount"]
            fees_paid = pos.get("fees_paid", pos["entry"] * pos["amount"] * self.fee_rate) + fee
            realized_pnl = pos.get("realized_pnl", 0.0) + pnl
            self.prices[symbol] = price
            self.balance += pnl - fee
            if full_close:
                del self.positions[symbol]
            else:
                self.positions[symbol] = {
                    **pos, "amount": pos["amount"] - amount,
                    "margin": pos["margin"] - released_margin,
                    "fees_paid": fees_paid, "realized_pnl": realized_pnl,
                }
            trade = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol, "side": side, "amount": amount,
                "price": price, "pnl_usdt": pnl, "fee_usdt": fee,
                "reduceOnly": True, "partial_close": not full_close,
                "remaining_amount": 0.0 if full_close else self.positions[symbol]["amount"],
                "released_margin_usdt": released_margin,
                "exit_reason": params.get("exit_reason"),
                "market_timestamp": ticker["timestamp"],
            }
            if full_close:
                trade["position_net_pnl_usdt"] = realized_pnl - fees_paid
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
                "fees_paid": old.get("fees_paid", old["entry"] * old["amount"] * self.fee_rate) + fee,
                "realized_pnl": old.get("realized_pnl", 0.0),
            }
        else:
            self.positions[symbol] = {
                "side": side,
                "entry": price,
                "amount": amount,
                "margin": margin,
                "original_entry": price,
                "original_notional": notional,
                "fees_paid": fee, "realized_pnl": 0.0,
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
        self.scan_status = {"phase": "starting", "cycle": 0, "checks": {}}
        self.scan_errors = {}
        self.entry_outcomes = {}
        self.scanner = ContinuousEntryScanner(self)
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
                    pos.setdefault("partial_done", False)
                    pos.setdefault("break_even_armed", False)
                    pos.setdefault("stop_kind", "TRAIL_SL" if pos["trailing"] else "SL")
                    # Older states had no partial exits; their entry fees are recoverable.
                    fill.setdefault("fees_paid", fill["entry"] * fill["amount"] * self.account.fee_rate)
                    fill.setdefault("realized_pnl", 0.0)
                    if pos["adds_done"]:
                        self.apply_tight_stop(pos)
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
            self.scan_status["universe_error"] = f"{type(e).__name__}: {e}"
            log.error(f"Top symbols error: {e}")
            return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    async def get_imbalance(self, symbol):
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=20)
            bid = sum(x[1] for x in ob["bids"][:20])
            ask = sum(x[1] for x in ob["asks"][:20])
            return (bid - ask) / (bid + ask) if (bid + ask) > 0 else 0.0
        except Exception as e:
            self.note_scan_error(symbol, "order_book", e)
            return 0.0

    def note_scan_error(self, symbol, stage, error):
        message = f"{type(error).__name__}: {error}"
        self.scan_errors.setdefault(symbol, []).append({"stage": stage, "message": message})
        log.warning("SCAN DATA ERROR %s | %s | %s", symbol, stage, message)

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
            except Exception as e:
                self.note_scan_error(symbol, "optional_5m", e)
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
            self.note_scan_error(symbol, "candles_1m_or_detection", e)
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

    async def enter(self, symbol, side, price, strength, *, signal_guard=None):
        now = time.time()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            self.entry_outcomes[symbol] = "COOLDOWN"
            return False
        if symbol in self.positions or len(self.positions) >= MAX_POSITIONS:
            self.entry_outcomes[symbol] = "ALREADY_OPEN_OR_POSITION_LIMIT"
            return False

        try:
            ticker = await self.fresh_ticker(symbol)
            if signal_guard is not None and not signal_guard():
                self.entry_outcomes[symbol] = "SIGNAL_CHANGED_OR_STREAM_STALE"
                return False
            amount, margin_pct, fill_price = await self.size_position(symbol, strength, ticker=ticker)
            if amount <= 0:
                self.entry_outcomes[symbol] = "QUANTITY_ROUNDED_TO_ZERO"
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
                "margin_pct": margin_pct,
                "partial_done": False,
                "break_even_armed": False,
                "stop_kind": "SL"
            }
            self.entry_outcomes[symbol] = "OPENED"
            log.info(f"ENTER {side.upper()} {symbol} | size={amount:.4f} @ {fill_price:.4f} | strength={strength:.3f} | margin={margin_pct*100:.1f}%")
            self.save_state()
            return True
        except Exception as e:
            self.entry_outcomes[symbol] = f"{type(e).__name__}: {e}"
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
            self.apply_tight_stop(p)
            self.account.trades[-1]["stop_loss_price"] = p["sl"]
            log.info("SCALE-IN %s | new avg=%.4f", symbol, p["entry"])
            self.save_state()
            return True
        except Exception as e:
            log.warning("Scale-in failed %s: %s", symbol, e)
            return False

    @staticmethod
    def tighten_stop(p, candidate, kind):
        improves = candidate > p["sl"] if p["side"] == "buy" else candidate < p["sl"]
        if improves:
            p["sl"] = candidate
            p["stop_kind"] = kind

    def apply_tight_stop(self, p):
        direction = 1 if p["side"] == "buy" else -1
        self.tighten_stop(p, p["entry"] * (1 - direction * POST_ADD_STOP_PCT), "TIGHT_SL")

    def break_even_price(self, symbol):
        # Cover all lifecycle fees and realized partial losses with the remaining quantity.
        fill = self.account.positions[symbol]
        direction = 1 if fill["side"] == "buy" else -1
        amount = fill["amount"]
        fees = fill["fees_paid"]
        realized = fill["realized_pnl"]
        return (direction * fill["entry"] * amount + fees - realized) / (
            amount * (direction - self.account.fee_rate))

    async def partial_close(self, symbol, *, ticker):
        try:
            p = self.positions[symbol]
            if p.get("partial_done") or not p["adds_done"]:
                return False
            price = validated_price(ticker)
            direction = 1 if p["side"] == "buy" else -1
            if direction * (price - p["sl"]) <= 0:
                return False
            adverse = -direction * (price / p["entry"] - 1)
            if adverse + 1e-12 < PARTIAL_ADVERSE_PCT:
                return False
            amount = float(self.exchange.amount_to_precision(symbol, p["amount"] * PARTIAL_CLOSE_FRACTION))
            if amount <= 0 or amount >= p["amount"]:
                log.warning("Partial close skipped %s: quantity too small to split", symbol)
                return False
            side = "sell" if p["side"] == "buy" else "buy"
            await self.account.create_order(
                symbol, "market", side, amount,
                params={"reduceOnly": True, "exit_reason": "PARTIAL_RISK"}, ticker=ticker)
            p["amount"] = self.account.positions[symbol]["amount"]
            p["partial_done"] = True
            self.save_state()
            log.info("PARTIAL_RISK %s | closed=%.8f remaining=%.8f", symbol, amount, p["amount"])
            return True
        except Exception as e:
            log.warning("Partial close failed %s: %s", symbol, e)
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
                if p["adds_done"]:
                    self.apply_tight_stop(p)
                if direction * (price - p["sl"]) <= 0:
                    await self.close(symbol, p.get("stop_kind", "SL"), price, ticker=ticker)
                    return
                adverse = -direction * (price / p["original_entry"] - 1)
                if not p["trailing"] and p["adds_done"] == 0 and adverse + 1e-12 >= ADD_LEVEL:
                    await self.add_position(symbol, price, ticker=ticker)
                # A gap can already be beyond the newly tightened stop; close all first.
                if direction * (price - p["sl"]) <= 0:
                    await self.close(symbol, p.get("stop_kind", "SL"), price, ticker=ticker)
                    return
                if p["adds_done"]:
                    await self.partial_close(symbol, ticker=ticker)
                    break_even = self.break_even_price(symbol)
                    p["break_even_price"] = break_even
                    if math.isfinite(break_even) and direction * (price - break_even) > 1e-12:
                        self.tighten_stop(p, break_even, "BE_SL")
                        p["break_even_armed"] = True
                gain = direction * (price / p["entry"] - 1)
                if not p["trailing"] and gain + 1e-12 >= TAKE_PROFIT_PCT:
                    p["trailing"] = True
                    p["best_price"] = price
                if p["trailing"]:
                    if direction == 1:
                        p["best_price"] = max(p["best_price"], price)
                    else:
                        p["best_price"] = min(p["best_price"], price)
                    self.tighten_stop(p, p["best_price"] * (1 - direction * TRAIL_DISTANCE), "TRAIL_SL")
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
        """Manual REST diagnostic sweep; live runtime uses the continuous scanner."""
        self.scan_status = {
            "phase": "scanning", "cycle": self.scan_status.get("cycle", 0) + 1,
            "started_at": time.time(), "finished_at": None, "next_scan_at": None,
            "total": 0, "checked": 0, "successful": 0, "failed": 0,
            "move_pass": 0, "volume_pass": 0, "book_pass": 0,
            "signals": 0, "entries": 0, "entry_attempts": 0,
            "entry_results": {}, "checks": {},
        }
        self.scan_errors = {}
        self.entry_outcomes = {}
        symbols = await self.get_top_symbols()
        self.scan_status["total"] = len(symbols)
        log.info(f"Scanning {len(symbols)} symbols...")
        signals = []
        for sym in symbols:
            sig = await self.detect(sym)
            errors = self.scan_errors.get(sym, [])
            failed = sig is None or any(e["stage"] != "optional_5m" for e in errors)
            row = {"symbol": sym, "checked_at": time.time(),
                   "status": "error" if failed else "checked", "errors": errors}
            if sig is not None:
                # Diagnostics do not alter the signal or its thresholds.
                for key in ("ret", "vol_spike", "imb", "strength"):
                    value = float(sig[key])
                    row[key] = value if math.isfinite(value) else None
                row["move_pass"] = bool(abs(sig["ret"]) > CASCADE_THRESHOLD)
                row["volume_pass"] = bool(sig["vol_spike"] > VOLUME_SPIKE)
                row["book_pass"] = bool((sig["ret"] > 0 and sig["imb"] > 0.35)
                                         or (sig["ret"] < 0 and sig["imb"] < -0.35))
                row["signal"] = "LONG" if sig["long"] else "SHORT" if sig["short"] else None
                for key in ("move", "volume", "book"):
                    self.scan_status[key + "_pass"] += int(row[key + "_pass"])
            if sig and (sig["long"] or sig["short"]):
                signals.append(sig)
            self.scan_status["checks"][sym] = row
            self.scan_status["checked"] += 1
            self.scan_status["failed" if failed else "successful"] += 1
            self.scan_status["signals"] = len(signals)
            await asyncio.sleep(0.15)

        signals.sort(key=lambda x: x["strength"], reverse=True)
        for sig in signals:
            async with self.lock:
                if len(self.positions) >= MAX_POSITIONS:
                    self.scan_status["position_limit_reached"] = True
                    break
                if sig["symbol"] in self.positions:
                    self.scan_status["entry_results"][sig["symbol"]] = "ALREADY_OPEN"
                    continue
                side = "buy" if sig["long"] else "sell"
                self.scan_status["entry_attempts"] += 1
                opened = await self.enter(sig["symbol"], side, sig["price"], sig["strength"])
                self.scan_status["entries"] += int(bool(opened))
                self.scan_status["entry_results"][sig["symbol"]] = self.entry_outcomes.get(
                    sig["symbol"], "OPENED" if opened else "REJECTED")
        self.scan_status["finished_at"] = time.time()
        log.info("SCAN RESULT | checked=%d/%d ok=%d errors=%d move=%d volume=%d book=%d signals=%d opened=%d",
                 self.scan_status["checked"], self.scan_status["total"], self.scan_status["successful"],
                 self.scan_status["failed"], self.scan_status["move_pass"], self.scan_status["volume_pass"],
                 self.scan_status["book_pass"], self.scan_status["signals"], self.scan_status["entries"])

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
        await self.scanner.run()

    async def run(self):
        log.info("Final-3 Pro started | continuous WebSocket watch; universe refresh 600s | 7% SL -> 4% post-add -> break-even | 50% partial at 3% | dynamic size 10-17% | light MTF")
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
    import fcntl
    state_path = STATE_FILE.resolve()
    with state_path.with_name(state_path.name + ".lock").open("a") as state_lock:
        try:
            fcntl.flock(state_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("My-life is already running on this state file")
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
