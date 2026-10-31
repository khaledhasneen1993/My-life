import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import test_scan
pro=test_scan.pro
NOW=1_800_000_000_000
BTC='BTC/USDT:USDT';ETH='ETH/USDT:USDT'

class StreamMarket(test_scan.Market):
    async def fetch_ohlcv(self,symbol,timeframe,limit):
        return [[NOW-(29-i)*60000,100,100,100,100,1] for i in range(30)]
    def market(self,symbol):
        return dict(linear=True,swap=True,settle='USDT',active=True,id=symbol.split(':')[0].replace('/',''))


def event(symbol='BTCUSDT',close=102,volume=10,stamp=NOW,offset=1):
    return {'e':'kline','E':stamp+offset,'s':symbol,'st':1,'k':{
        's':symbol,'i':'1m','t':stamp,'o':'100','h':str(max(100,close)),
        'l':str(min(100,close)),'c':str(close),'v':str(volume)}}


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.previous=pro.STATE_FILE
        pro.STATE_FILE=Path(self.tmp.name)/'state.json';self.market=StreamMarket()
        with patch.object(pro.ccxt,'binanceusdm',return_value=self.market):self.bot=pro.Final3Pro()
        self.mono=100.;self.wall=NOW/1000
        self.sc=pro.ContinuousEntryScanner(self.bot,clock=lambda:self.mono,wall_clock=lambda:self.wall)
        self.bot.scanner=self.sc
        await self.sc.refresh_universe();await self.sc.seed(self.sc.symbols);self.sc.connected=True
    async def asyncTearDown(self):
        pro.STATE_FILE=self.previous;self.tmp.cleanup()

    async def test_new_signal_is_seen_between_universe_refreshes(self):
        self.sc.ingest(event(close=100,volume=1));await self.sc.evaluate(BTC)
        self.assertFalse(self.bot.positions);self.assertEqual(self.sc.book_checks,0)
        self.mono+=2;self.wall+=2
        self.sc.ingest(event(close=102,volume=10,offset=2001));await self.sc.evaluate(BTC)
        self.assertIn(BTC,self.bot.positions);self.assertEqual(self.sc.entries,1)
        self.assertEqual(self.sc.next_universe_at,NOW/1000+600)

    async def test_mtf_boost_and_dynamic_size_preserved(self):
        self.sc.ingest(event(volume=20));await self.sc.evaluate(BTC)
        trade=self.bot.account.trades[-1]
        self.assertAlmostEqual(trade['strength'],.02*20*.5*1.12)
        self.assertAlmostEqual(trade['margin_pct'],.17)
        self.assertEqual(trade['entry_signal']['market_event_time'],NOW+1)

    async def test_five_minute_bias_uses_bucket_closes(self):
        rows=list(self.sc.histories[BTC])
        for row in rows:row[4]=100+(row[0]//300000)%4
        self.sc.histories[BTC].clear();self.sc.histories[BTC].extend(rows)
        buckets={row[0]//300000:row[4] for row in rows};closes=list(buckets.values())
        self.assertAlmostEqual(self.sc.metrics(BTC)['mtf'],(closes[-1]-closes[-3])/closes[-3])

    async def test_weak_signal_does_not_request_order_book(self):
        self.sc.ingest(event(close=100.2,volume=1));await self.sc.evaluate(BTC)
        self.assertEqual(self.sc.book_checks,0);self.assertFalse(self.bot.positions)
        self.assertFalse(self.sc.checks[BTC]['move_pass'])

    async def test_order_book_is_still_required(self):
        self.sc.ingest(event())
        async def balanced(*args,**kwargs):return {'bids':[[100,1]],'asks':[[100,1]]}
        self.market.fetch_order_book=balanced
        await self.sc.evaluate(BTC);self.assertFalse(self.bot.positions)
        self.assertFalse(self.sc.checks[BTC]['book_pass'])

    async def test_signal_is_rechecked_after_order_book_await(self):
        self.sc.ingest(event());original=self.market.fetch_order_book
        async def book(*args,**kwargs):
            self.sc.ingest(event(close=100,offset=2))
            return await original(*args,**kwargs)
        self.market.fetch_order_book=book
        await self.sc.evaluate(BTC);self.assertFalse(self.bot.positions)

    async def test_stale_or_disconnected_stream_cannot_open(self):
        self.sc.ingest(event());self.mono+=11;self.wall+=11
        await self.sc.evaluate(BTC);self.assertFalse(self.bot.positions)
        self.mono=100;self.wall=NOW/1000;self.sc.connected=False
        await self.sc.evaluate(BTC);self.assertFalse(self.bot.positions)

    async def test_signal_is_rechecked_after_execution_ticker_await(self):
        self.sc.ingest(event())
        original=self.market.fetch_ticker
        async def ticker(symbol):
            self.sc.ingest(event(close=100,offset=2))
            return await original(symbol)
        self.market.fetch_ticker=ticker
        await self.sc.evaluate(BTC)
        self.assertFalse(self.bot.positions)
        self.assertEqual(self.sc.entry_results[BTC],'SIGNAL_CHANGED_OR_STREAM_STALE')

    async def test_execution_ticker_keeps_independent_12_second_guard(self):
        self.sc.ingest(event());self.market.age=13
        await self.sc.evaluate(BTC);self.assertFalse(self.bot.positions)
        self.assertIn('Stale ticker',self.sc.entry_results[BTC])

    async def test_duplicate_frames_do_not_duplicate_orders(self):
        for n in range(1,20):self.sc.ingest(event(offset=n))
        self.assertEqual(self.sc.queue.qsize(),1)
        await asyncio.gather(self.sc.evaluate(BTC),self.sc.evaluate(BTC))
        self.assertEqual(len(self.bot.account.trades),1)

    async def test_removed_currency_keeps_its_position(self):
        self.sc.ingest(event());await self.sc.evaluate(BTC)
        async def tickers():return {ETH:{'quoteVolume':20_000_000}}
        self.market.fetch_tickers=tickers
        await self.sc.refresh_universe()
        self.assertEqual(self.sc.symbols,(ETH,));self.assertIn(BTC,self.bot.positions)
        self.assertIsNone(self.sc.signal(BTC))

    async def test_candle_gap_and_old_event_are_rejected(self):
        self.sc.ingest(event(offset=5));self.sc.ingest(event(close=110,offset=4))
        self.assertEqual(self.sc.histories[BTC][-1][4],102)
        self.wall+=120
        with self.assertRaises(pro.CandleGap):self.sc.ingest(event(stamp=NOW+120000))

    async def test_failed_order_book_backoff_is_visible(self):
        self.sc.ingest(event())
        async def failed(*args,**kwargs):raise ConnectionError('offline')
        self.market.fetch_order_book=failed
        await self.sc.evaluate(BTC)
        self.assertEqual(self.sc.book_pause_until,105)
        self.assertIn('offline',self.sc.diagnostics()['last_error'])
        self.assertEqual(self.sc.checks[BTC]['status'],'error')

    async def test_socket_reconnect_reseeds_and_cancels_cleanly(self):
        opened=asyncio.Event();tracker={'opens':0,'seeds':0,'closed':False};seed=self.sc.seed
        async def counted(symbols):tracker['seeds']+=1;return await seed(symbols)
        self.sc.seed=counted
        class Socket:
            async def __aenter__(self):
                tracker['opens']+=1;self.number=tracker['opens']
                if self.number==2:opened.set()
                return self
            async def __aexit__(self,*args):pass
            def __aiter__(self):
                async def messages():
                    if self.number==1:raise ConnectionError('disconnect fixture')
                    await asyncio.Future()
                    yield None
                return messages()
        class Session:
            async def __aenter__(self):return self
            async def __aexit__(self,*args):tracker['closed']=True
            def ws_connect(self,*args,**kwargs):return Socket()
        with patch.object(pro.aiohttp,'ClientSession',return_value=Session()):
            task=asyncio.create_task(self.sc.stream_loop())
            try:await asyncio.wait_for(opened.wait(),3)
            finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.assertGreaterEqual(tracker['seeds'],2);self.assertTrue(tracker['closed']);self.assertFalse(self.sc.connected)

    async def test_silent_socket_watchdog_requests_reconnect(self):
        self.sc.ingest(event('BTCUSDT'));self.sc.ingest(event('ETHUSDT'))
        self.mono+=16;self.wall+=16
        with patch.object(pro,'WATCHDOG_INTERVAL',.001):
            with self.assertRaises(ConnectionError):await asyncio.wait_for(self.sc.freshness_watchdog(self.sc.symbols),.1)

    async def test_healthy_currency_not_blocked_by_one_quiet_currency(self):
        self.sc.ingest(event('BTCUSDT'));self.sc.ingest(event('ETHUSDT'))
        self.mono+=16;self.wall+=16;self.sc.ingest(event(offset=16001))
        with patch.object(pro,'WATCHDOG_INTERVAL',.001):
            task=asyncio.create_task(self.sc.freshness_watchdog(self.sc.symbols))
            await asyncio.sleep(.005)
            self.assertFalse(task.done());self.assertTrue(self.sc.connected)
            task.cancel();await asyncio.gather(task,return_exceptions=True)

    async def test_exit_watcher_runs_while_continuous_scanner_is_busy(self):
        await self.bot.enter(BTC,'buy',102,.05)
        async def ticker(symbol):return {'last':90,'timestamp':time.time()*1000}
        self.market.fetch_ticker=ticker
        started=asyncio.Event()
        async def scanner():started.set();await asyncio.Future()
        async def load_markets():return {}
        async def close():self.market.closed=True
        self.market.load_markets=load_markets;self.market.close=close
        with patch.object(self.bot.scanner,'run',side_effect=scanner),patch.object(pro,'EXIT_INTERVAL',.001):
            task=asyncio.create_task(self.bot.run())
            try:
                await started.wait()
                for _ in range(100):
                    if not self.bot.positions:break
                    await asyncio.sleep(.001)
                self.assertFalse(self.bot.positions)
            finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(self.market.closed)

    async def test_dashboard_reports_actual_websocket_health(self):
        import worker
        self.sc.ingest(event())
        snapshot=worker.snapshot_state('mylife',self.bot,worker.pro_settings(pro),'running',{})
        self.assertEqual(snapshot['scanner']['transport'],'websocket')
        self.assertEqual(snapshot['scanner']['events'],1)
        self.assertEqual(snapshot['scanner']['fresh_symbols'],1)
        self.assertEqual(snapshot['scan_report']['mode'],'continuous')
        self.assertEqual(snapshot['settings']['universe_refresh'],600)

    async def test_universe_loop_waits_600_only_after_refresh(self):
        calls=[]
        async def stop_wait(seconds):calls.append(seconds);raise asyncio.CancelledError()
        with patch.object(pro.asyncio,'sleep',side_effect=stop_wait):
            with self.assertRaises(asyncio.CancelledError):await self.sc.universe_loop()
        self.assertEqual(calls,[600]);self.assertTrue(self.sc.ready.is_set())

if __name__=='__main__':unittest.main()
