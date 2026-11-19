"""Signal ledger tests (SGNL bot). Run: python3 test_signal_dedupe.py"""
import os, sys, tempfile, time, types
for m in ("mplfinance", "websocket"):
    try: __import__(m)
    except Exception: sys.modules.setdefault(m, types.ModuleType(m))
import bot
tmp = tempfile.mkdtemp()
bot.SIGNAL_LEDGER_FILE = os.path.join(tmp, "signal_ledger.json")
bar, B5 = 1763395200000, 300000
chk = lambda gs, sym, tf, side, b, e: bot.signal_ledger_check(gs, sym, tf, side, b, e)
assert chk({}, "ATOM/USD", "4h", "SELL", bar, 1.32) is None
bot.signal_ledger_claim({}, "ATOM/USD", "4h", "SELL", bar, 1.32, "ALT-000976", "native")
assert "ALT-000976" in chk({}, "ATOM/USD", "4h", "SELL", bar, 1.32)                    # same candle
assert chk({}, "ATOMUSDT", "4H", "SELL", bar, 1.5) is not None                          # spelling variants, same candle
assert chk({}, "ATOM/USD", "4h", "BUY", bar, 1.32) is None                              # other side
assert chk({}, "SOL/USD", "4h", "SELL", bar, 1.32) is None                              # other symbol
assert chk({}, "ATOM/USD", "1h", "SELL", bar, 1.6) is None                              # other TF + far price = separate signal
assert chk({}, "ATOM/USD", "1h", "SELL", bar, 1.322) is not None                        # other TF + same price, still open = duplicate setup
assert chk({}, "ATOM/USD", "4h", "SELL", bar + 14400000, 1.322) is not None             # next bar, same price
# live case: NIGHT 5M SHORT twice, 10 minutes apart, first still open (state lost => status unknown)
bot.signal_ledger_claim({}, "NIGHT/USD", "5m", "SELL", bar, 0.04342, "ALT-000982", "native")
assert "still open" in chk({}, "NIGHT/USD", "5m", "SELL", bar + 2 * B5, 0.04292)
# ...but once the first trade is known to be closed (trade history), a new signal is allowed
closed = {"trade_history": [{"signal_id": "ALT-000982"}]}
assert chk(closed, "NIGHT/USD", "5m", "SELL", bar + 2 * B5, 0.04292) is None
# live case: NIGHT 15M then 5M, price within 0.6%, still open => 5M blocked; closed => allowed
bot.signal_ledger_claim({}, "WIF/USD", "15m", "SELL", bar, 0.04362, "ALT-000981", "native")
assert "already open on 15M" in chk({}, "WIF/USD", "5m", "SELL", bar + B5, 0.04350)
assert chk({"trade_history": [{"signal_id": "ALT-000981"}]}, "WIF/USD", "5m", "SELL", bar + 3 * B5, 0.04350) is None
# an open native trade known to the state keeps blocking
open_state = {"candle_signals": {"NIGHT/USD|5m": {"open_trade": {"signal_id": "ALT-000982", "closed": False}}}}
assert chk(open_state, "NIGHT/USD", "5m", "SELL", bar + 2 * B5, 0.04) is not None
# survives restart (fresh file read) and expires
assert chk(None, "ATOM/USD", "4h", "SELL", bar, 1.32) is not None
items = bot._ledger_items()
for r in items: r["t"] -= 25 * 3600 * 1000
bot._save_small_json_file(bot.SIGNAL_LEDGER_FILE, {"items": items})
assert chk({}, "ATOM/USD", "4h", "SELL", bar, 1.32) is None and chk({}, "NIGHT/USD", "5m", "SELL", bar, 0.0429) is None
open(bot.SIGNAL_LEDGER_FILE, "w").write("{bad")                                          # corrupt file never blocks
assert chk({}, "ATOM/USD", "4h", "SELL", bar, 1.32) is None
# native vs an open MODULE trade on another timeframe (live case: ZEC 15M vs 5M)
gsm = {"module_store": {"module_trades.json": {"trades": {"ALT-1": {"symbol": "ZEC/USD", "side": "SELL", "tf": "5m", "entry": 935.79, "status": "open", "opened_ms": int(time.time() * 1000) - 300000}}}}}
assert bot.find_module_duplicate(gsm, "ZEC/USD", "15m", "SELL", 936.57)[0] == "ALT-1"
assert bot.find_module_duplicate(gsm, "ZEC/USD", "15m", "SELL", 990.0)[0] is None
assert bot.find_module_duplicate(gsm, "ZEC/USD", "15m", "BUY", 936.57)[0] is None
assert bot.find_module_duplicate(gsm, "ZEC/USD", "5m", "SELL")[0] == "ALT-1"                    # same TF still blocks without a price
print("SIGNAL DEDUPE TESTS PASSED")
