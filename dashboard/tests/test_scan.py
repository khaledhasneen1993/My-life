import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

spec = importlib.util.spec_from_file_location('scan_test_pro', Path(__file__).resolve().parents[2] / 'final3_pro.py')
pro = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pro)


class Market:
    def __init__(self):
        self.active = False
        self.fail_stage = None
        self.age = 0

    async def fetch_tickers(self):
        return {'BTC/USDT:USDT': {'quoteVolume': 20_000_000},
                'ETH/USDT:USDT': {'quoteVolume': 19_000_000}}

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        if self.fail_stage == '1m' and timeframe == '1m':raise RuntimeError('Candle request failed')
        if self.fail_stage == '5m' and timeframe == '5m':raise RuntimeError('Optional MTF request failed')
        rows = [[int(time.time()*1000),100,103,99,100,10] for _ in range(limit)]
        if self.active:rows[-1] = [int(time.time()*1000),100,103,99,102,50]
        return rows

    async def fetch_order_book(self, symbol, limit):
        if self.fail_stage == 'book':raise RuntimeError('Order book request failed')
        return {'bids': [[100,30]], 'asks': [[101,10]]}

    async def fetch_ticker(self, symbol):
        return {'last': 102, 'timestamp': (time.time()-self.age)*1000}

    def amount_to_precision(self, symbol, amount):return round(amount,8)


class ScanTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.previous = pro.STATE_FILE
        pro.STATE_FILE = Path(self.tmp.name) / 'state.json'
        self.market = Market()
        with patch.object(pro.ccxt, 'binanceusdm', return_value=self.market):self.bot = pro.Final3Pro()

    async def asyncTearDown(self):
        pro.STATE_FILE = self.previous
        self.tmp.cleanup()

    async def cycle(self):
        with patch.object(pro.asyncio, 'sleep', new=AsyncMock()):await self.bot.scan_cycle()
        json.dumps(self.bot.scan_status, allow_nan=False)
        return self.bot.scan_status

    async def test_no_signal_is_reported_as_successful_checks_without_orders(self):
        report = await self.cycle()
        self.assertEqual(report['checked'],2);self.assertEqual(report['successful'],2)
        self.assertEqual(report['failed'],0);self.assertEqual(report['signals'],0)
        self.assertEqual(report['entries'],0);self.assertFalse(self.bot.account.trades)
        self.assertFalse(report['checks']['BTC/USDT:USDT']['move_pass'])

    async def test_failed_candles_are_visible_and_not_silently_successful(self):
        self.market.fail_stage = '1m'
        with self.assertLogs(pro.log, level='WARNING') as logs:report = await self.cycle()
        self.assertEqual(report['failed'],2);self.assertEqual(report['successful'],0)
        self.assertEqual(report['checks']['BTC/USDT:USDT']['status'],'error')
        self.assertIn('Candle request failed',' '.join(logs.output))

    async def test_book_errors_are_distinct_from_unmet_signal(self):
        self.market.active = True;self.market.fail_stage = 'book'
        report = await self.cycle()
        self.assertEqual(report['failed'],2);self.assertEqual(report['signals'],0)
        row = report['checks']['BTC/USDT:USDT']
        self.assertTrue(row['move_pass']);self.assertTrue(row['volume_pass'])
        self.assertEqual(row['errors'][0]['stage'],'order_book')

    async def test_optional_mtf_failure_keeps_original_entry_behavior(self):
        self.market.active = True;self.market.fail_stage = '5m'
        report = await self.cycle()
        self.assertEqual(report['successful'],2);self.assertEqual(report['signals'],2)
        self.assertEqual(report['entries'],2);self.assertEqual(report['failed'],0)
        self.assertAlmostEqual(report['checks']['BTC/USDT:USDT']['strength'],.05)
        self.assertEqual(report['entry_results']['BTC/USDT:USDT'],'OPENED')

    async def test_original_thresholds_and_mtf_boost_stay_the_same(self):
        self.market.active = True
        report = await self.cycle()
        self.assertEqual(report['signals'],2);self.assertEqual(report['entries'],2)
        self.assertAlmostEqual(report['checks']['BTC/USDT:USDT']['strength'],.05*1.12)
        self.assertEqual(pro.CASCADE_THRESHOLD,.012);self.assertEqual(pro.VOLUME_SPIKE,3.5)
        self.assertEqual(pro.SCAN_INTERVAL,600)

    async def test_stale_execution_is_reported_as_rejection(self):
        self.market.active = True;self.market.age = 13
        report = await self.cycle()
        self.assertEqual(report['signals'],2);self.assertEqual(report['entries'],0)
        self.assertEqual(report['entry_attempts'],2)
        self.assertIn('Stale ticker',report['entry_results']['BTC/USDT:USDT'])
        self.assertFalse(self.bot.account.trades)

    async def test_live_loop_uses_continuous_scanner_without_600_second_pause(self):
        with patch.object(self.bot.scanner, 'run', new=AsyncMock()) as run:
            await self.bot.scan_loop()
            run.assert_awaited_once()


if __name__ == '__main__':unittest.main()
