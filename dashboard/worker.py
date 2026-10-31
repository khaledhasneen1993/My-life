"""Wrap each upstream bot in a separate process; strategy source remains unchanged."""
import asyncio
import fcntl
import json
import logging
import signal
import sys
import time
from paths import NAMES, LABELS, RUNTIME, SOURCES, STATE_PATHS, archive_path
from ledger import FillArchive

log = logging.getLogger('DASHBOARD')


def pro_settings(module):
    settings = {key: getattr(module, key) for key in (
        'LEVERAGE', 'CASCADE_THRESHOLD', 'VOLUME_SPIKE', 'TAKE_PROFIT_PCT',
        'STOP_LOSS_PCT', 'COOLDOWN_SEC', 'SCAN_INTERVAL', 'MIN_QUOTE_VOLUME')}
    settings.update(mode='final3_pro', MAX_POSITION_PCT=module.BASE_MARGIN_PCT,
        MAX_MARGIN_PCT=module.MAX_MARGIN_PCT, MAX_CONCURRENT_POSITIONS=module.MAX_POSITIONS,
        MAX_SYMBOLS_TO_SCAN=module.MAX_SYMBOLS, add_levels=[module.ADD_LEVEL], add_fraction=1.0,
        trail_activation=module.TAKE_PROFIT_PCT, trail_distance=module.TRAIL_DISTANCE,
        post_add_stop=module.POST_ADD_STOP_PCT, partial_adverse=module.PARTIAL_ADVERSE_PCT,
        partial_fraction=module.PARTIAL_CLOSE_FRACTION, max_price_age=module.MAX_PRICE_AGE,
        size_formula='0.10 + 0.07 * clamp((strength - 0.09) / 0.09, 0, 1)')
    return settings


def snapshot_state(name, bot, settings, status, ticker_health):
    if name == 'mylife':
        account = bot.account
        state = dict(initial_balance_usdt=100.0, balance_usdt=account.balance,
            equity_usdt=account.equity, free_usdt=account.free, used_usdt=account.used,
            account_positions=account.positions, strategy_positions=bot.positions,
            prices=account.prices, trades=account.trades[-150:], settings=settings,
            fee_rate=account.fee_rate, last_trade=bot.last_trade)
        now = time.time()
        symbols = sorted(set(bot.positions) | set(ticker_health))
        health = [dict(symbol=s, age=now-ticker_health[s]['received'] if s in ticker_health else None,
                       event_age=now-ticker_health[s]['market']/1000 if s in ticker_health else None,
                       seeded=True) for s in symbols]
        for h in health:
            h['fresh'] = h['age'] is not None and h['age'] <= 12 and abs(h['event_age']) <= 12
        state['scanner'] = dict(transport='rest', connected=None, symbols=symbols,
            events=sum(h['events'] for h in ticker_health.values()), entries=None, book_checks=None,
            last_event_age=min((h['age'] for h in health if h['age'] is not None), default=None),
            fresh_symbols=sum(h['fresh'] for h in health), symbol_health=health,
            freshness_limit=12)
    else:
        account = bot.exchange
        state = dict(initial_balance_usdt=100.0, balance_usdt=account.balance,
            equity_usdt=account.equity, free_usdt=account.free, used_usdt=account.used,
            account_positions=account.positions, strategy_positions=bot.positions,
            prices=account.prices, trades=account.trades, settings=bot.settings(),
            fee_rate=account.fee_rate, last_trade=bot.last_trade)
        sc = bot.scanner
        mono, wall = sc.clock(), sc.wall_clock()
        health = []
        for symbol in sc.symbols:
            received, event = sc.received.get(symbol), sc.event_times.get(symbol)
            age = mono-received if received is not None else None
            event_age = wall-event/1000 if event is not None else None
            health.append(dict(symbol=symbol, age=age, event_age=event_age,
                fresh=bool(sc.connected and age is not None and age <= 10
                           and event_age is not None and -10 <= event_age <= 10),
                seeded=symbol in sc.histories))
        state['scanner'] = dict(transport='websocket', connected=sc.connected,
            symbols=list(sc.symbols), events=sc.events, entries=sc.entries, book_checks=sc.book_checks,
            last_event_age=min((h['age'] for h in health if h['age'] is not None), default=None),
            fresh_symbols=sum(h['fresh'] for h in health), symbol_health=health, freshness_limit=10)
    state.update(bot=name, status=status, updated_at=time.time(), state_path=str(STATE_PATHS[name]))
    return state


async def run_worker(name):
    sys.path.insert(0, str(SOURCES[name]))
    if name == 'mylife':
        import final3_pro as module
        module.STATE_FILE = STATE_PATHS[name]
        bot = module.Final3Pro()
        settings = pro_settings(module)
        account, start, save = bot.account, bot.run, bot.save_state
    else:
        if name == 'final':
            from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter as Hunter
        else:
            from cascade_final3_paper import Final3PaperHunter as Hunter
        bot = Hunter(state_path=STATE_PATHS[name])
        account, start, save = bot.exchange, bot.start, bot.save
        settings = bot.settings()
    archive = FillArchive(archive_path(name))
    health = {}
    ready = False
    market_client = bot.exchange
    original_load = market_client.load_markets
    async def observed_load(*args, **kwargs):
        nonlocal ready
        result = await original_load(*args, **kwargs)
        ready = True
        return result
    market_client.load_markets = observed_load
    archive_error = None
    original_order = account.create_order

    def sync_archive():
        nonlocal archive_error
        try:
            archive.sync(account.trades)
            archive_error = None
        except Exception as exc:
            archive_error = str(exc)
            log.error('Execution archive update failed; saved state retained: %s', exc)

    async def archived_order(*args, **kwargs):
        fill = await original_order(*args, **kwargs)
        # Capture every fill before the 150-item state window can discard it.
        sync_archive()
        return fill

    account.create_order = archived_order
    if name == 'mylife':
        original_ticker = bot.fresh_ticker
        async def observed_ticker(symbol):
            ticker = await original_ticker(symbol)
            health[symbol] = dict(received=time.time(), market=ticker['timestamp'],
                                  events=health.get(symbol, {}).get('events', 0)+1)
            return ticker
        bot.fresh_ticker = observed_ticker

    def publish(status):
        sync_archive()
        state = snapshot_state(name, bot, settings, status, health)
        state['archive_error'] = archive_error
        target = RUNTIME / 'data' / f'{name}.snapshot.json'
        temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(state, ensure_ascii=False, allow_nan=False), encoding='utf-8')
        temporary.replace(target)

    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    async def report():
        while True:
            publish('running' if ready else 'starting')
            await asyncio.sleep(3)
    tasks = []
    status = 'stopped'
    try:
        save()
        publish('starting')
        tasks = [asyncio.create_task(start()), asyncio.create_task(report()),
                 asyncio.create_task(stopped.wait())]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in tasks[:2]:
            if task in done:
                await task
    except Exception:
        status = 'error'
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            save()
            publish(status)
        finally:
            archive.close()


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in NAMES:
        raise SystemExit('Choose final, final3 or mylife')
    name = sys.argv[1]
    path = STATE_PATHS[name]
    path.parent.mkdir(parents=True, exist_ok=True)
    (RUNTIME / 'data').mkdir(parents=True, exist_ok=True)
    with (RUNTIME / 'data' / f'{name}.lock').open('a') as process_lock, path.with_name(path.name+'.lock').open('a') as state_lock:
        try:
            for handle in (process_lock, state_lock):
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f'{LABELS[name]} is already running; no duplicate worker started')
        asyncio.run(run_worker(name))


if __name__ == '__main__':
    main()
