
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tradeiscool Bot - نسخه یکپارچه ۴.۰
====================================
سه نوع سیگنال را در یک ربات تلگرام ادغام می‌کند:
  1) سیگنال جریان استیبل‌کوین (Cryptometer)               -> هر ۳ ساعت
  2) سیگنال عملکرد نسبی به بیت‌کوین (CoinMarketCap)         -> هر ۳ ساعت
  3) سیگنال کندلی تایم‌فریم ۴ ساعته (اندیکاتور Pine Script)  -> هر ۴ ساعت
     (ترجمه‌ی دقیق منطق اندیکاتور به پایتون + داده‌ی کندل از Binance،
      بدون نیاز به وبهوک تریدینگ‌ویو یا پلن پولی)
 
طراحی‌شده برای اجرا از طریق GitHub Actions (کاملاً رایگان، بدون نیاز به هاست):
هر بار که اجرا می‌شود، فقط کارهای متناسب با ساعت فعلی UTC را انجام می‌دهد و خارج
می‌شود (بدون حلقه‌ی بی‌نهایت). وضعیت لازم برای جلوگیری از سیگنال تکراری در
فایل state.json نگه‌داری و توسط خودِ ورک‌فلو به‌صورت خودکار در ریپازیتوری
کامیت می‌شود.
 
نکته مهم: قابلیت «فرستادن اسم ارز به ربات و گرفتن گزارش» (که در نسخه‌ی قبلی بود)
در این نسخه حذف شده، چون نیاز به یک فرآیند همیشه-روشن دارد که با مدل اجرای
دوره‌ای GitHub Actions سازگار نیست.
 
نکته دیگر: در نسخه‌ی قبلی، وقتی دریافت داده از Cryptometer شکست می‌خورد، ربات
داده‌های «نمونه»‌ی تصادفی (random.random) تولید و آن‌ها را عیناً مثل یک سیگنال
واقعی ارسال می‌کرد. این رفتار در این نسخه حذف شده، چون فرستادن سیگنال ساختگی
که ظاهرش با سیگنال واقعی فرقی ندارد می‌تواند تصمیم معاملاتی را گمراه کند. حالا
اگر داده‌ی واقعی در دسترس نباشد، آن دور اسکن به‌سادگی رد می‌شود.
"""
 
import os
import re
import time
import json
import logging
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
 
# -------------------- تنظیمات محیطی --------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
 
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID', '').strip()
COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY', '').strip()
LIVECOINWATCH_API_KEY = os.getenv('LIVECOINWATCH_API_KEY', '').strip()
CRYPTOMETER_API_URL = os.getenv('CRYPTOMETER_API_URL', 'https://cryptometer.io').strip()
TWELVEDATA_API_KEY = (os.getenv('TD_API_KEY', '').strip() or os.getenv('TWELVEDATA_API_KEY', '').strip())
# برای تست دستی از طریق workflow_dispatch: همه‌ی اسکن‌ها را صرف‌نظر از ساعت اجرا کن
FORCE_RUN_ALL = os.getenv('FORCE_RUN_ALL', '').strip() == '1'
 
STABLECOINS = ['USDT', 'USDC', 'FDUSD', 'USD', 'BUSD', 'DAI', 'TUSD', 'USDP', 'USDD']
 
# زمان‌بندی بر اساس ساعت UTC (هر بار GitHub Actions اجرا شود، این لیست‌ها چک می‌شوند)
SCAN_SCHEDULE_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]   # جریان استیبل‌کوین + عملکرد نسبی، هر ۳ ساعت
CANDLE_SCAN_HOURS = [0, 4, 8, 12, 16, 20]            # سیگنال کندلی، هر ۴ ساعت (هم‌راستا با بسته‌شدن کندل ۴ساعته)
 
HTTP_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.5
 
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
 
# -------------------- لاگ‌گیری --------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("TradeiscoolBot")
 
 
# ==================================================================
# توابع کمکی عمومی
# ==================================================================
def safe_get(d: Dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d
 
 
def retry_request(method: str, url: str, **kwargs):
    attempt = 0
    while attempt < RETRY_ATTEMPTS:
        attempt += 1
        try:
            resp = requests.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    return resp.text
            else:
                logger.warning(f"HTTP {resp.status_code} برای {url}")
        except Exception as e:
            logger.warning(f"خطای درخواست برای {url}: {e}")
        time.sleep(RETRY_BACKOFF * attempt)
    logger.error(f"تعداد تلاش‌ها برای {url} بیش از حد مجاز")
    return None
 
 
def format_number(value: float) -> str:
    if value == 0:
        return "۰"
    elif abs(value) >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    elif abs(value) >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    elif abs(value) >= 1_000:
        return f"${value/1_000:.1f}K"
    else:
        return f"${value:.0f}"
 
 
def format_percent(value: float) -> str:
    return f"{value:+.2f}%"
 
 
def format_price(price: float) -> str:
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.6f}"
    else:
        return f"${price:.8f}"
 
 
def clean_number_string(s: Any) -> Tuple[float, str]:
    if s is None:
        return 0.0, ''
    if isinstance(s, (int, float)):
        return float(s), ''
    s = str(s).strip()
    s_nocomma = s.replace(',', '').replace('\u200b', '')
    m = re.search(r'([-+]?\d{1,3}(?:[\d,]*\d)?(?:\.\d+)?)(?:\s*([A-Za-z]{2,8}))?$', s_nocomma)
    if m:
        num = m.group(1)
        unit = (m.group(2) or '').upper()
        try:
            return float(num), unit
        except Exception:
            return 0.0, unit
    m2 = re.search(r'([-+]?\d+\.?\d*)', s_nocomma)
    if m2:
        try:
            return float(m2.group(1)), ''
        except Exception:
            return 0.0, ''
    return 0.0, ''
 
 
def is_stable_unit(u: str) -> bool:
    if not u:
        return False
    return u.upper() in STABLECOINS
 
 
def normalize_symbol(raw: Any) -> str:
    if raw is None:
        return ''
    if isinstance(raw, dict):
        for k in ['symbol', 'base', 'code', 'id', 'ticker', 'asset', 'coin']:
            if k in raw and raw[k]:
                return normalize_symbol(raw[k])
        return ''
    s = str(raw).strip().replace('"', '').replace("'", "").strip()
    if '/' in s:
        return s.split('/')[0].upper()
    if '-' in s:
        return s.split('-')[0].upper()
    if s.upper() in STABLECOINS:
        return s.upper()
    if s.upper().endswith('USD') and len(s) > 3:
        if s.upper() in STABLECOINS:
            return s.upper()
        return re.sub(r'USD$', '', s, flags=re.IGNORECASE).upper()
    return s.upper()
 
 
# ==================================================================
# ذخیره‌سازی وضعیت (برای جلوگیری از سیگنال تکراری بین اجراهای مجزا)
# ==================================================================
def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ خطا در خواندن state.json، شروع با وضعیت خالی: {e}")
    return {}
 
 
def save_state(state: Dict[str, Any]):
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        logger.info("💾 state.json ذخیره شد")
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره state.json: {e}")
 
 
# ==================================================================
# ماژول جریان استیبل‌کوین (Cryptometer) - بدون تغییر منطقی نسبت به نسخه قبلی
# ==================================================================
class AdvancedCryptometerFetcher:
    CANDIDATE_API_ENDPOINTS = [
        "/api/v1/flows",
        "/api/v1/inflow-outflow",
        "/api/flows",
        "/api/v1/coin/flows",
        "/api/v1/exchange-flows",
        "/api/data/flow",
        "/api/stablecoin-flows"
    ]
 
    def __init__(self, session: requests.Session, base_url: str = ""):
        self.session = session
        self.base_url = base_url.rstrip('/')
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://cryptometer.io/'
        })
 
    def fetch_raw_data(self) -> Optional[Any]:
        endpoints = []
        if CRYPTOMETER_API_URL:
            endpoints.append(CRYPTOMETER_API_URL)
        if self.base_url:
            for path in self.CANDIDATE_API_ENDPOINTS:
                endpoints.append(self.base_url + path)
        if self.base_url:
            endpoints.append(self.base_url)
        else:
            endpoints.append("https://cryptometer.io/")
 
        seen = set()
        unique_endpoints = []
        for e in endpoints:
            if e and e not in seen:
                seen.add(e)
                unique_endpoints.append(e)
 
        for url in unique_endpoints:
            try:
                r = self.session.get(url, timeout=HTTP_TIMEOUT)
                if r.status_code != 200:
                    continue
                try:
                    return r.json()
                except Exception:
                    return r.text
            except Exception as e:
                logger.warning(f"⚠️ خطا در دریافت از {url}: {e}")
                continue
        logger.error("❌ تمام اندپوینت‌های Cryptometer امتحان شدند و نتیجه‌ای نیامد")
        return None
 
    def find_data_list(self, data: Any) -> List[Any]:
        possible_paths = [
            ['data'], ['coins'], ['result'], ['items'],
            ['list'], ['assets'], ['rows'], ['payload'], ['symbols']
        ]
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for path in possible_paths:
                current = data
                found = True
                for key in path:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        found = False
                        break
                if found and isinstance(current, list):
                    return current
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], (dict, str, int, float)):
                        return value
        return []
 
    def extract_flow_values(self, entry: Any) -> Tuple[float, float]:
        inflow = 0.0
        outflow = 0.0
        if not isinstance(entry, (dict, list)):
            return inflow, outflow
 
        inflow_keys = [
            'inflow_24h', 'inflow', 'inflow24h', 'inflowUsd', 'inflow_usd',
            'total_inflow', 'total_inflow_24h', 'buy_volume', 'buy_volume_24h',
            'volume_inflow', 'volume_inflow_24h', 'inflow_amount', 'inflow_value',
            'inflow_USD', 'inflow_usd_24h', 'buy_inflow', 'buy_inflow_24h'
        ]
        outflow_keys = [
            'outflow_24h', 'outflow', 'outflow24h', 'outflowUsd', 'outflow_usd',
            'total_outflow', 'total_outflow_24h', 'sell_volume', 'sell_volume_24h',
            'volume_outflow', 'volume_outflow_24h', 'outflow_amount', 'outflow_value',
            'outflow_USD', 'outflow_usd_24h', 'sell_outflow', 'sell_outflow_24h'
        ]
 
        for key in inflow_keys:
            if key in entry and entry[key] is not None:
                value, unit = clean_number_string(entry[key])
                if (unit and is_stable_unit(unit)) or not unit:
                    inflow += value
 
        for key in outflow_keys:
            if key in entry and entry[key] is not None:
                value, unit = clean_number_string(entry[key])
                if (unit and is_stable_unit(unit)) or not unit:
                    outflow += value
 
        if inflow == 0 and outflow == 0:
            net_flow_keys = [
                'netFlow', 'net_flow', 'netflow', 'net_flow_24h',
                'net_volume', 'netVolume', 'net_amount', 'netAmount', 'net',
                'net_flow_usd', 'netFlowUSD', 'net_flow_24h_usd'
            ]
            for key in net_flow_keys:
                if key in entry and entry[key] is not None:
                    value, unit = clean_number_string(entry[key])
                    if (unit and is_stable_unit(unit)) or not unit:
                        if value >= 0:
                            inflow = value
                        else:
                            outflow = abs(value)
                        break
 
        flow_list_keys = ['flows', 'stable_flows', 'flow_list', 'flowEntries', 'flow_items']
        for list_key in flow_list_keys:
            if list_key in entry and isinstance(entry[list_key], list):
                for flow_item in entry[list_key]:
                    try:
                        if not isinstance(flow_item, dict):
                            continue
                        amount = None
                        currency = ''
                        curr_from_val = ''
                        direction = ''
                        for amt_key in ['amount', 'value', 'volume', 'amt', 'qty', 'amount_usd', 'usd']:
                            if amt_key in flow_item and flow_item[amt_key] is not None:
                                amount, curr_from_val = clean_number_string(flow_item[amt_key])
                                break
                        for curr_key in ['currency', 'coin', 'symbol', 'unit']:
                            if curr_key in flow_item and flow_item[curr_key]:
                                currency = str(flow_item[curr_key]).upper()
                                break
                        if not currency:
                            currency = curr_from_val
                        for dir_key in ['direction', 'side', 'type']:
                            if dir_key in flow_item and flow_item[dir_key]:
                                direction = str(flow_item[dir_key]).lower()
                                break
                        if not direction:
                            if 'in' in flow_item and flow_item['in'] is not None:
                                amount, _ = clean_number_string(flow_item['in'])
                                direction = 'in'
                            elif 'out' in flow_item and flow_item['out'] is not None:
                                amount, _ = clean_number_string(flow_item['out'])
                                direction = 'out'
                        if amount is not None:
                            if not currency or is_stable_unit(currency):
                                if direction in ('in', 'inflow', 'buy'):
                                    inflow += amount
                                elif direction in ('out', 'outflow', 'sell'):
                                    outflow += amount
                                else:
                                    if amount >= 0:
                                        inflow += amount
                                    else:
                                        outflow += abs(amount)
                    except Exception:
                        continue
 
        return float(inflow or 0.0), float(outflow or 0.0)
 
    def extract_embedded_json(self, html_text: str) -> List[Any]:
        json_blocks = []
        patterns = [
            r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
            r'window\.__DATA__\s*=\s*({.+?});',
            r'var\s+INITIAL_STATE\s*=\s*({.+?});',
            r'window\.__SSR_DATA__\s*=\s*({.+?});',
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>({.+?})</script>',
            r'(\[{"symbol".+?}])'
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html_text, re.IGNORECASE | re.DOTALL):
                try:
                    json_str = match.group(1)
                    start = json_str.find('{')
                    end = json_str.rfind('}')
                    if start != -1 and end != -1 and end > start:
                        candidate = json_str[start:end + 1]
                        json_blocks.append(json.loads(candidate))
                except Exception:
                    continue
        return json_blocks
 
    def parse_data_payload(self, raw_data: Any, target_symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        results = []
        if raw_data is None:
            return results
 
        def _process(data_list):
            for item in data_list:
                try:
                    symbol = self.extract_symbol(item)
                    if not symbol or symbol in STABLECOINS:
                        continue
                    if target_symbols and symbol not in target_symbols:
                        continue
                    inflow, outflow = self.extract_flow_values(item)
                    net_flow = inflow - outflow
                    if abs(net_flow) >= 1000:
                        results.append({
                            'symbol': symbol,
                            'name': self.extract_name(item, symbol),
                            'inflow': inflow,
                            'outflow': outflow,
                            'net_flow': net_flow,
                            'raw_data': item
                        })
                except Exception:
                    continue
 
        if isinstance(raw_data, str):
            for json_block in self.extract_embedded_json(raw_data):
                _process(self.find_data_list(json_block))
        else:
            _process(self.find_data_list(raw_data))
 
        return results
 
    def extract_symbol(self, entry: Any) -> str:
        if isinstance(entry, dict):
            for key in ['symbol', 'base', 'ticker', 'code', 'id', 'asset', 'coin']:
                if key in entry and entry[key]:
                    symbol = normalize_symbol(entry[key])
                    if symbol:
                        return symbol
            for key in ['pair', 'market_pair', 'symbol_pair', 'market']:
                if key in entry and entry[key]:
                    symbol = normalize_symbol(entry[key])
                    if symbol:
                        return symbol
        elif isinstance(entry, str):
            return normalize_symbol(entry)
        return ''
 
    def extract_name(self, entry: Any, symbol: str) -> str:
        if isinstance(entry, dict):
            for key in ['name', 'Name', 'full_name', 'fullname', 'asset_name', 'base_currency_name']:
                if key in entry and entry[key]:
                    return str(entry[key]).strip()
        return symbol
 
    def fetch_flow_data(self, target_symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        try:
            raw_data = self.fetch_raw_data()
            if raw_data is None:
                return []
            parsed_data = self.parse_data_payload(raw_data, target_symbols)
            filtered_data = [
                item for item in parsed_data
                if item.get('symbol') and item['symbol'] not in STABLECOINS
                and (not target_symbols or item['symbol'] in target_symbols)
            ]
            filtered_data.sort(key=lambda x: abs(x['net_flow']), reverse=True)
            return filtered_data[:20]
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده‌های جریان: {e}")
            return []
 
 
# ==================================================================
# موتور سیگنال کندلی تایم‌فریم ۴ ساعته (ترجمه اندیکاتور Pine Script)
# منبع داده: Twelve Data (برای همه‌ی ۵ نماد - کریپتو و کالا)
#
# نکته مهم: در ابتدا برای بیت‌کوین/اتریوم از API خودِ Binance استفاده می‌شد، ولی
# Binance به‌طور رسمی دسترسی به API عمومی‌اش را از IP آمریکا مسدود کرده (خطای
# HTTP 451) و سرورهای GitHub Actions همیشه IP آمریکا دارند - این یک محدودیت
# دائمی از طرف Binance است و هیچ راه‌حل کدی ندارد. به همین دلیل همه‌ی ۵ نماد
# (کریپتو + کالا) از طریق Twelve Data خوانده می‌شوند که یک IP آمریکایی نیست
# که Binance بلاکش کند و خودش از ۱۸۰+ صرافی/بروکر (از جمله Binance) داده جمع می‌کند.
# ==================================================================
CANDLE_INTERVAL = "4h"
BOOTSTRAP_LIMIT = 300     # تعداد کندل برای گرم‌کردن اولیه EMA/ATR هنگام اولین اجرا برای هر نماد
COOLDOWN_BARS = 5         # cooldownBars در اسکریپت اصلی
WHIPSAW_ATR_MULT = 0.5    # minMoveATRMultiplier در اسکریپت اصلی
EMA_SLOPE_ATR_MULT = 0.03  # جایگزینِ نسبی و مقیاس‌پذیرِ emaSlopeThreshold ثابتِ اسکریپت اصلی (توضیح در step_candle_state)
CANDLE_BODY_MAX_RATIO = 0.5
SHADOW_RATIO = 3.5
 
# لیست کامل نمادهایی که سیگنال کندلی برایشان بررسی می‌شود (کلید رایگان لازم است:
# https://twelvedata.com). برای اضافه/حذف نماد، همین دیکشنری را ویرایش کنید.
WATCHLIST_SYMBOLS = {
    "BTC/USD": "بیت‌کوین (BTC)",
    "ETH/USD": "اتریوم (ETH)",
    "XAU/USD": "طلا (Gold)",
    "XAG/USD": "نقره (Silver)",
    "XCU/USD": "مس (Copper)",
}
TWELVEDATA_BASE = "https://api.twelvedata.com"
 
 
def fetch_closed_klines_twelvedata(symbol: str, limit: int) -> List[Dict[str, Any]]:
    """کندل‌های ۴ساعته‌ی بسته‌شده برای هر نماد (کریپتو یا کالا) از Twelve Data"""
    if not TWELVEDATA_API_KEY:
        return []
    url = f"{TWELVEDATA_BASE}/time_series"
    params = {
        "symbol": symbol,
        "interval": CANDLE_INTERVAL,
        "outputsize": limit,
        "timezone": "UTC",
        "apikey": TWELVEDATA_API_KEY,
    }
    data = retry_request("GET", url, params=params)
    if not data or not isinstance(data, dict):
        return []
    if data.get("status") == "error":
        logger.warning(f"⚠️ Twelve Data برای {symbol} خطا داد: {data.get('message')} "
                        f"(احتمالاً این نماد نیاز به پلن پولی دارد)")
        return []
    values = data.get("values")
    if not values:
        return []
    now = time.time()
    candles = []
    for v in values:
        try:
            dt = datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            open_time = int(dt.timestamp() * 1000)
            # اگر کندل هنوز کامل نشده (کمتر از ۴ ساعت از بازشدنش گذشته) رد شود
            if dt.timestamp() + 4 * 3600 > now:
                continue
            o, h, l, c = float(v["open"]), float(v["high"]), float(v["low"]), float(v["close"])
            candles.append({"open_time": open_time, "o": o, "h": h, "l": l, "c": c})
        except Exception:
            continue
    candles.sort(key=lambda k: k["open_time"])  # از قدیم به جدید
    return candles
 
 
def new_candle_state() -> Dict[str, Any]:
    return {
        "ema7": None, "ema25": None,
        "atr": None, "tr_buffer": [],
        "hist": [],
        "trend_prev": "flat",
        "bull_used_this_trend": False,
        "bear_used_this_trend": False,
        "last_bull_bar_index": None,
        "last_bear_bar_index": None,
        "last_signal_price": None,
        "bar_index": 0,
        "last_open_time": None,
    }
 
 
def _ema_step(prev: Optional[float], price: float, length: int) -> float:
    if prev is None:
        return price
    alpha = 2.0 / (length + 1)
    return alpha * price + (1 - alpha) * prev
 
 
def step_candle_state(state: Dict[str, Any], o: float, h: float, l: float, c: float,
                       open_time: int) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    پردازش یک کندل جدید بسته‌شده و به‌روزرسانی وضعیت - معادل دقیقِ اجرای بار-به-بارِ
    منطق اندیکاتور Pine Script (بدون هیچ تغییری در شرایط کندل سیگنال، فقط فیلتر
    ضد تکرارِ خودِ اسکریپت روی خروجی اعمال شده است).
    """
    s = dict(state)
    s["hist"] = list(s["hist"])
    s["tr_buffer"] = list(s["tr_buffer"])
 
    ema7_prev = s["ema7"]
    ema25_prev = s["ema25"]
 
    ema7_i = _ema_step(ema7_prev, c, 7)
    ema25_i = _ema_step(ema25_prev, c, 25)
 
    hist = s["hist"]
    prev_bar = hist[-1] if len(hist) >= 1 else None
    prev2_bar = hist[-2] if len(hist) >= 2 else None
 
    # True Range / ATR(14) - Wilder
    tr_i = max(h - l, abs(h - prev_bar["c"]), abs(l - prev_bar["c"])) if prev_bar else (h - l)
    if s["atr"] is None:
        s["tr_buffer"].append(tr_i)
        atr_i = sum(s["tr_buffer"][-14:]) / 14.0 if len(s["tr_buffer"]) >= 14 else None
    else:
        atr_i = (s["atr"] * 13 + tr_i) / 14.0
 
    body = abs(c - o)
    upper_shadow = h - max(c, o)
    lower_shadow = min(c, o) - l
    total_size = h - l
 
    is_uptrend = ema7_i > ema25_i
    is_downtrend = ema7_i < ema25_i
 
    is_valid_bull_candle = (body < CANDLE_BODY_MAX_RATIO * total_size) and (lower_shadow > SHADOW_RATIO * upper_shadow)
    is_valid_bear_candle = (body < CANDLE_BODY_MAX_RATIO * total_size) and (upper_shadow > SHADOW_RATIO * lower_shadow)
 
    next_invalidates_bull = (prev_bar is not None) and (prev_bar["l"] < l)
    next_invalidates_bear = (prev_bar is not None) and (prev_bar["h"] > h)
 
    ema7_slope = ema7_i - (ema7_prev if ema7_prev is not None else ema7_i)
    # نکته: در اسکریپت اصلی Pine، این آستانه یک عدد ثابت (0.001) بود که کاربر
    # به‌صورت دستی برای یک نماد خاص در تریدینگ‌ویو تنظیم می‌کرد. وقتی همین عدد
    # ثابت را روی صدها نماد با مقیاس قیمتی بسیار متفاوت (از آلت‌کوین‌های زیر
    # ۰.۰۰۰۱ دلار تا بیت‌کوین با قیمت شش‌رقمی) اعمال کنیم، عملاً اکثر آلت‌کوین‌ها
    # را همیشه «فلت» تشخیص می‌دهد (چون شیب EMA آن‌ها در واحد دلار خیلی کوچک است)
    # و سیگنال‌هایشان را مسدود می‌کند. به همین دلیل این آستانه را نسبت به ATR
    # (که خودش با مقیاس هر نماد هماهنگ می‌شود) تعریف کردیم تا فیلتر برای همه‌ی
    # نمادها منصفانه عمل کند.
    is_ema7_flat = True if atr_i is None else abs(ema7_slope) < (EMA_SLOPE_ATR_MULT * atr_i)
 
    bullish_engulf = bearish_engulf = False
    if prev_bar is not None:
        po, pc = prev_bar["o"], prev_bar["c"]
        bullish_engulf = (c > o) and (pc < po) and (c > po) and (o < pc)
        bearish_engulf = (c < o) and (pc > po) and (c < po) and (o > pc)
 
    bullish_pin = (lower_shadow > 2 * body) and (upper_shadow < body)
    bearish_pin = (upper_shadow > 2 * body) and (lower_shadow < body)
 
    both_above = both_below = False
    if prev_bar is not None and prev2_bar is not None:
        both_above = (prev_bar["c"] > prev_bar["ema7"]) and (prev2_bar["c"] > prev2_bar["ema7"])
        both_below = (prev_bar["c"] < prev_bar["ema7"]) and (prev2_bar["c"] < prev2_bar["ema7"])
 
    raw_bull = is_uptrend and is_valid_bull_candle and (not next_invalidates_bull) and (not is_ema7_flat) and both_above
    raw_bear = is_downtrend and is_valid_bear_candle and (not next_invalidates_bear) and (not is_ema7_flat) and both_below
 
    if is_uptrend and s["trend_prev"] != "up":
        s["bull_used_this_trend"] = False
    if is_downtrend and s["trend_prev"] != "down":
        s["bear_used_this_trend"] = False
 
    state_ok_bull = not s["bull_used_this_trend"]
    state_ok_bear = not s["bear_used_this_trend"]
 
    cooldown_ok_bull = (s["last_bull_bar_index"] is None) or (s["bar_index"] - s["last_bull_bar_index"] >= COOLDOWN_BARS)
    cooldown_ok_bear = (s["last_bear_bar_index"] is None) or (s["bar_index"] - s["last_bear_bar_index"] >= COOLDOWN_BARS)
 
    if s["last_signal_price"] is None or atr_i is None:
        price_move_ok = True
    else:
        price_move_ok = abs(c - s["last_signal_price"]) >= atr_i * WHIPSAW_ATR_MULT
 
    final_bull = raw_bull and state_ok_bull and cooldown_ok_bull and price_move_ok
    final_bear = raw_bear and state_ok_bear and cooldown_ok_bear and price_move_ok
 
    signal = None
    if final_bull:
        s["bull_used_this_trend"] = True
        s["last_bull_bar_index"] = s["bar_index"]
        s["last_signal_price"] = c
        signal = {"side": "BUY", "confirmed": bool(bullish_engulf or bullish_pin), "price": c, "open_time": open_time}
    if final_bear:
        s["bear_used_this_trend"] = True
        s["last_bear_bar_index"] = s["bar_index"]
        s["last_signal_price"] = c
        signal = {"side": "SELL", "confirmed": bool(bearish_engulf or bearish_pin), "price": c, "open_time": open_time}
 
    hist.append({"o": o, "h": h, "l": l, "c": c, "ema7": ema7_i})
    s["hist"] = hist[-2:]
    s["ema7"] = ema7_i
    s["ema25"] = ema25_i
    s["atr"] = atr_i
    s["trend_prev"] = "up" if is_uptrend else ("down" if is_downtrend else "flat")
    s["bar_index"] = s["bar_index"] + 1
    s["last_open_time"] = open_time
 
    return s, signal
 
 
def process_symbol_candles(fetch_fn, symbol: str,
                            sym_state: Optional[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    fetch_fn(symbol, limit) -> List[candle dict] باید کندل‌های بسته‌شده را برگرداند
    (یک تابع مشترک برای هر دو منبع داده‌ی Binance و Twelve Data).
 
    اگر برای این نماد وضعیت قبلی وجود نداشته باشد (اولین اجرا) -> بوت‌استرپ با ۳۰۰ کندل
    اگر وضعیت قبلی وجود دارد -> فقط کندل‌های جدیدِ بسته‌شده از آخرین بار پردازش می‌شوند
    """
    if sym_state is None:
        candles = fetch_fn(symbol, BOOTSTRAP_LIMIT)
        if len(candles) < 30:
            return None, []
        state = new_candle_state()
        signals = []
        last_idx = len(candles) - 1
        for idx, k in enumerate(candles):
            state, sig = step_candle_state(state, k["o"], k["h"], k["l"], k["c"], k["open_time"])
            if sig and idx == last_idx:
                signals.append(sig)
        return state, signals
    else:
        last_open_time = sym_state.get("last_open_time")
        candles = fetch_fn(symbol, 10)
        new_candles = [k for k in candles if last_open_time is None or k["open_time"] > last_open_time]
        state = sym_state
        signals = []
        for k in new_candles:
            state, sig = step_candle_state(state, k["o"], k["h"], k["l"], k["c"], k["open_time"])
            if sig:
                signals.append(sig)
        return state, signals
 
 
def format_candle_signal_message(symbol: str, sig: Dict[str, Any], display_name: Optional[str] = None) -> str:
    side = sig["side"]
    confirmed = sig["confirmed"]
    price = sig["price"]
    ts = datetime.fromtimestamp(sig["open_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if side == "BUY":
        emoji, title, action = "🟢", "سیگنال خرید (کندلی)", "📈"
    else:
        emoji, title, action = "🔴", "سیگنال فروش (کندلی)", "📉"
    confirm_txt = "✅ تأییدشده (اینگولفینگ/پین‌بار)" if confirmed else "⚪ بدون تأیید اضافه"
    if display_name:
        clean_symbol = display_name
    else:
        clean_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
 
    return f"""
{emoji} <b>{title}</b> {action}
<b>مبتنی بر اندیکاتور کندلی - تایم‌فریم ۴ ساعته</b>
 
<b>💰 ارز:</b> {clean_symbol} ({symbol})
<b>💵 قیمت کندل سیگنال:</b> {format_price(price)}
<b>🔎 وضعیت تأیید:</b> {confirm_txt}
<b>⏰ زمان کندل:</b> {ts}
 
⚠️ <b>این یک سیگنال خام است؛ قبل از ورود حتماً بررسی و مدیریت ریسک را انجام دهید.</b>
"""
 
 
def candle_signal_scan_job(bot: "TradeiscoolBot", state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    اسکن سیگنال کندلی برای واچ‌لیست کریپتو + کالاها.
    برخلاف نسخه‌ی قبلی، این نسخه برای هرکدام از نمادهای رصدشده یک گزارش وضعیت
    برمی‌گرداند (موفق/ناموفق/سیگنال/دلیل سکوت) تا مشکلات به‌جای سکوت مبهم قابل دیدن باشند.
    """
    logger.info("🕯️ شروع اسکن سیگنال‌های کندلی (تایم‌فریم ۴ ساعته)...")
    candle_states = state.setdefault("candle_signals", {})
    sent_count = 0
    report: List[Dict[str, Any]] = []
 
    def _describe_silence(new_state: Dict[str, Any]) -> str:
        """چرا الان سیگنالی نیامد - برای گزارش تشخیصی"""
        if not new_state:
            return "داده کافی نیست"
        trend = new_state.get("trend_prev")
        trend_fa = "صعودی" if trend == "up" else ("نزولی" if trend == "down" else "خنثی")
        if trend == "up" and new_state.get("bull_used_this_trend"):
            return f"روند {trend_fa} - سیگنال این روند قبلاً ارسال شده (تا تغییر روند، سیگنال جدید نمی‌آید)"
        if trend == "down" and new_state.get("bear_used_this_trend"):
            return f"روند {trend_fa} - سیگنال این روند قبلاً ارسال شده (تا تغییر روند، سیگنال جدید نمی‌آید)"
        return f"روند فعلی: {trend_fa} - شکل کندل اخیر با شرایط ورود اندیکاتور مطابقت نداشت"
 
    if not TWELVEDATA_API_KEY:
        logger.warning("⚠️ TWELVEDATA_API_KEY تنظیم نشده - کل اسکن کندلی (هر ۵ نماد) رد شد")
        for symbol, display_name in WATCHLIST_SYMBOLS.items():
            report.append({"symbol": symbol, "display_name": display_name, "ok": False,
                            "signal": None, "error": "TWELVEDATA_API_KEY تنظیم نشده", "note": None})
    else:
        logger.info(f"🔎 در حال بررسی {len(WATCHLIST_SYMBOLS)} نماد از Twelve Data...")
        fetch_fn = lambda sym, lim: fetch_closed_klines_twelvedata(sym, lim)
        for symbol, display_name in WATCHLIST_SYMBOLS.items():
            entry = {"symbol": symbol, "display_name": display_name, "ok": False,
                      "signal": None, "error": None, "note": None}
            try:
                sym_state = candle_states.get(symbol)
                new_state, signals = process_symbol_candles(fetch_fn, symbol, sym_state)
                if new_state is None:
                    entry["error"] = "دریافت داده از Twelve Data ناموفق بود (ممکن است این نماد نیاز به پلن پولی داشته باشد)"
                else:
                    candle_states[symbol] = new_state
                    entry["ok"] = True
                    entry["note"] = _describe_silence(new_state)
                    for sig in signals:
                        message = format_candle_signal_message(symbol, sig, display_name=display_name)
                        if bot.send_telegram_message(message):
                            logger.info(f"📤 سیگنال کندلی {symbol} ({sig['side']}) ارسال شد")
                            sent_count += 1
                            entry["signal"] = sig["side"]
                            time.sleep(1.2)
                time.sleep(1)  # رعایت محدودیت نرخ درخواست Twelve Data (۸ درخواست در دقیقه در پلن رایگان)
            except Exception as e:
                entry["error"] = str(e)
                logger.warning(f"⚠️ خطا در پردازش {symbol}: {e}")
            report.append(entry)
 
    logger.info("——— گزارش تشخیصی اسکن کندلی ———")
    for e in report:
        if e["signal"]:
            logger.info(f"  {e['symbol']} ({e['display_name']}): ✅ سیگنال {e['signal']} ارسال شد")
        elif e["error"]:
            logger.info(f"  {e['symbol']} ({e['display_name']}): ❌ خطا - {e['error']}")
        elif e["ok"]:
            logger.info(f"  {e['symbol']} ({e['display_name']}): ⚪ بدون سیگنال جدید - {e['note']}")
    logger.info(f"✅ اسکن کندلی کامل شد. {sent_count} سیگنال ارسال شد.")
    return report
 
 
# ==================================================================
# کلاس اصلی ربات: سیگنال‌های جریان استیبل‌کوین + عملکرد نسبی
# ==================================================================
class TradeiscoolBot:
    def __init__(self, state: Dict[str, Any]):
        self.session = requests.Session()
        self.top_coins_cache = None
        self.top_coins_cache_time = None
        self._state_ref = state
        self.flow_signals_sent = {
            k: datetime.fromisoformat(v) for k, v in state.get("flow_signals_sent", {}).items()
        }
        self.performance_signals_sent = {
            k: datetime.fromisoformat(v) for k, v in state.get("performance_signals_sent", {}).items()
        }
        self.crypto_fetcher = AdvancedCryptometerFetcher(self.session, "https://cryptometer.io")
 
    def sync_state(self):
        """نتایج در حافظه را قبل از ذخیره‌ی نهایی به دیکشنری state برمی‌گرداند"""
        self._state_ref["flow_signals_sent"] = {k: v.isoformat() for k, v in self.flow_signals_sent.items()}
        self._state_ref["performance_signals_sent"] = {k: v.isoformat() for k, v in self.performance_signals_sent.items()}
 
    def send_telegram_message(self, message: str, chat_id: str = None) -> bool:
        if not TELEGRAM_BOT_TOKEN:
            logger.error("تنظیمات تلگرام وجود ندارد")
            return False
        if not chat_id:
            chat_id = TELEGRAM_CHANNEL_ID
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
        try:
            resp = self.session.post(url, data=payload, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200 and resp.json().get("ok"):
                return True
            logger.error(f"❌ خطا در ارسال تلگرام: {resp.text}")
            return False
        except Exception as e:
            logger.error(f"❌ خطا در ارسال به تلگرام: {e}")
            return False
 
    # ---------------- CoinMarketCap ----------------
    def get_top_coins_from_cmc(self, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            if (self.top_coins_cache and self.top_coins_cache_time and
                    (datetime.now() - self.top_coins_cache_time).total_seconds() < 3600):
                return self.top_coins_cache
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
            headers = {'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY, 'Accept': 'application/json'}
            params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
            data = retry_request("GET", url, headers=headers, params=params)
            if data and isinstance(data, dict):
                coins = data.get('data', [])
                filtered_coins = [c for c in coins if c['symbol'] not in STABLECOINS]
                self.top_coins_cache = filtered_coins
                self.top_coins_cache_time = datetime.now()
                return filtered_coins
            return []
        except Exception as e:
            logger.error(f"❌ خطا در دریافت لیست ارزهای برتر: {e}")
            return []
 
    # ---------------- LiveCoinWatch ----------------
    def get_coin_detailed_data(self, symbols: List[str]) -> Dict[str, Dict]:
        try:
            url = "https://api.livecoinwatch.com/coins/list"
            headers = {'content-type': 'application/json', 'x-api-key': LIVECOINWATCH_API_KEY}
            payload = {"currency": "USD", "sort": "rank", "order": "ascending", "limit": 500, "offset": 0}
            data = retry_request("POST", url, headers=headers, json=payload)
            if isinstance(data, list):
                coins_data = data
            elif isinstance(data, dict) and "data" in data:
                coins_data = data['data']
            else:
                return {}
            symbol_map = {c.get('code', '').upper(): c for c in coins_data}
            coin_details = {}
            for symbol in symbols:
                cd = symbol_map.get(symbol)
                if cd:
                    volume = cd.get('volume', 0) or 0
                    market_cap = cd.get('cap', 0) or 0
                    price = cd.get('rate', 0) or 0
                    volume_ratio = (volume / market_cap * 100) if market_cap > 0 else 0
                    coin_details[symbol] = {
                        'volume_24h': volume, 'market_cap': market_cap, 'price': price,
                        'volume_ratio': volume_ratio, 'name': cd.get('name', symbol)
                    }
            return coin_details
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده از LiveCoinWatch: {e}")
            return {}
 
    # ---------------- سیگنال عملکرد نسبی ----------------
    def get_performance_based_signals(self) -> List[Dict[str, Any]]:
        try:
            top_coins = self.get_top_coins_from_cmc(100)
            if not top_coins:
                return []
            btc_data = next((c for c in top_coins if c['symbol'] == 'BTC'), None)
            if not btc_data:
                return []
            btc_change = safe_get(btc_data, "quote", "USD", "percent_change_24h") or 0
 
            performance_data = []
            for coin in top_coins:
                try:
                    symbol = coin['symbol']
                    if symbol in STABLECOINS:
                        continue
                    coin_change = safe_get(coin, "quote", "USD", "percent_change_24h") or 0
                    relative_change = coin_change - btc_change
                    quote_data = safe_get(coin, "quote", "USD", default={})
                    performance_data.append({
                        'symbol': symbol, 'name': coin.get('name', ''),
                        'price': quote_data.get('price', 0),
                        'volume_24h': quote_data.get('volume_24h', 0),
                        'market_cap': quote_data.get('market_cap', 0),
                        'change_24h': coin_change, 'relative_performance': relative_change,
                        'rank': coin.get('cmc_rank', 0)
                    })
                except Exception:
                    continue
 
            gainers = [c for c in performance_data if c['relative_performance'] > 2 and c['volume_24h'] > 10_000_000]
            losers = [c for c in performance_data if c['relative_performance'] < -2 and c['volume_24h'] > 10_000_000]
            gainers.sort(key=lambda x: x['relative_performance'], reverse=True)
            losers.sort(key=lambda x: x['relative_performance'])
 
            signals = []
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for coin in gainers[:3]:
                signals.append({**coin, 'side': 'LONG', 'type': 'PERFORMANCE', 'timestamp': now_str})
            for coin in losers[:3]:
                signals.append({**coin, 'side': 'SHORT', 'type': 'PERFORMANCE', 'timestamp': now_str})
            return signals
        except Exception as e:
            logger.error(f"❌ خطا در دریافت سیگنال‌های عملکردی: {e}")
            return []
 
    def get_btc_relative_performance(self, symbols: List[str]) -> Dict[str, float]:
        try:
            top_coins = self.get_top_coins_from_cmc(200)
            if not top_coins:
                return {}
            btc_data = next((c for c in top_coins if c['symbol'] == 'BTC'), None)
            if not btc_data:
                return {}
            btc_change = safe_get(btc_data, "quote", "USD", "percent_change_24h") or 0
            symbol_map = {c['symbol']: c for c in top_coins}
            result = {}
            for symbol in symbols:
                cd = symbol_map.get(symbol)
                if cd:
                    coin_change = safe_get(cd, "quote", "USD", "percent_change_24h") or 0
                    result[symbol] = coin_change - btc_change
                else:
                    result[symbol] = 0
            return result
        except Exception as e:
            logger.error(f"❌ خطا در دریافت عملکرد نسبی: {e}")
            return {}
 
    # ---------------- امتیازدهی ----------------
    def calculate_flow_score(self, net_flow: float) -> int:
        abs_flow = abs(net_flow)
        if abs_flow >= 10_000_000: return 3
        if abs_flow >= 3_000_000: return 2
        if abs_flow >= 1_000_000: return 1
        return 0
 
    def calculate_volume_ratio_score(self, volume_ratio: float) -> int:
        if volume_ratio >= 15: return 3
        if volume_ratio >= 8: return 2
        if volume_ratio >= 3: return 1
        return 0
 
    def calculate_relative_performance_score(self, relative_change: float, side: str) -> int:
        if side == 'LONG':
            if relative_change >= 8: return 3
            if relative_change >= 4: return 2
            if relative_change >= 2: return 1
            return 0
        else:
            if relative_change <= -8: return 3
            if relative_change <= -4: return 2
            if relative_change <= -2: return 1
            return 0
 
    def calculate_performance_rank_score(self, rank: int) -> int:
        if rank <= 10: return 3
        if rank <= 30: return 2
        if rank <= 50: return 1
        return 0
 
    def calculate_market_cap_score(self, market_cap: float) -> int:
        if market_cap >= 10_000_000_000: return 3
        if market_cap >= 1_000_000_000: return 2
        if market_cap >= 100_000_000: return 1
        return 0
 
    def calculate_total_score(self, signal_data: Dict[str, Any]) -> int:
        if signal_data['type'] == 'FLOW':
            stage1 = self.calculate_flow_score(signal_data['net_flow'])
            stage2 = self.calculate_volume_ratio_score(signal_data.get('volume_ratio', 0))
            stage3 = self.calculate_relative_performance_score(signal_data.get('relative_performance', 0), signal_data['side'])
            stage4 = self.calculate_market_cap_score(signal_data.get('market_cap', 0))
        else:
            stage1 = self.calculate_relative_performance_score(signal_data.get('relative_performance', 0), signal_data['side'])
            stage2 = self.calculate_volume_ratio_score(signal_data.get('volume_ratio', 0))
            stage3 = self.calculate_performance_rank_score(signal_data.get('rank', 0))
            stage4 = self.calculate_market_cap_score(signal_data.get('market_cap', 0))
        total = stage1 + stage2 + stage3 + stage4
        signal_data.update({'stage1_score': stage1, 'stage2_score': stage2, 'stage3_score': stage3,
                             'stage4_score': stage4, 'total_score': total})
        return total
 
    # ---------------- جریان استیبل‌کوین ----------------
    def get_cryptometer_data(self, target_symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        داده‌ی جریان واقعی را برمی‌گرداند. اگر دریافت داده شکست بخورد، لیست خالی
        برمی‌گرداند (بدون تولید داده‌ی ساختگی) تا هیچ سیگنال نادرستی ارسال نشود.
        """
        try:
            flow_data = self.crypto_fetcher.fetch_flow_data(target_symbols)
            if not flow_data:
                logger.warning("⚠️ داده‌ی واقعی جریان استیبل‌کوین در دسترس نبود؛ این دور اسکن رد می‌شود")
                return []
            filtered_data = [item for item in flow_data if abs(item.get('net_flow', 0)) >= 1000]
            for item in filtered_data:
                item['side'] = 'LONG' if item.get('net_flow', 0) > 0 else 'SHORT'
                item['type'] = 'FLOW'
            return filtered_data
        except Exception as e:
            logger.error(f"❌ خطا در دریافت داده از Cryptometer: {e}")
            return []
 
    def scan_for_flow_signals(self) -> List[Dict[str, Any]]:
        try:
            flow_coins = self.get_cryptometer_data()
            if not flow_coins:
                return []
            symbols = [c['symbol'] for c in flow_coins]
            coin_details = self.get_coin_detailed_data(symbols)
            relative_performance = self.get_btc_relative_performance(symbols)
 
            valid_signals = []
            for coin in flow_coins:
                symbol, side = coin['symbol'], coin['side']
                if self.is_duplicate_flow_signal(symbol, side, hours=3):
                    continue
                details = coin_details.get(symbol, {})
                signal_data = {
                    'symbol': symbol, 'name': coin['name'], 'side': side, 'type': 'FLOW',
                    'net_flow': coin['net_flow'], 'inflow': coin['inflow'], 'outflow': coin['outflow'],
                    'volume_24h': details.get('volume_24h', 0), 'market_cap': details.get('market_cap', 0),
                    'volume_ratio': details.get('volume_ratio', 0), 'price': details.get('price', 0),
                    'relative_performance': relative_performance.get(symbol, 0),
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                total_score = self.calculate_total_score(signal_data)
                if total_score >= 4:
                    valid_signals.append(signal_data)
 
            valid_signals.sort(key=lambda x: x['total_score'], reverse=True)
            for signal in valid_signals:
                self.flow_signals_sent[f"FLOW_{signal['symbol']}_{signal['side']}"] = datetime.now()
            return valid_signals[:5]
        except Exception as e:
            logger.error(f"❌ خطا در اسکن سیگنال‌های جریانی: {e}")
            return []
 
    def is_duplicate_flow_signal(self, symbol: str, side: str, hours: int = 3) -> bool:
        key = f"FLOW_{symbol}_{side}"
        if key in self.flow_signals_sent:
            return (datetime.now() - self.flow_signals_sent[key]).total_seconds() < hours * 3600
        return False
 
    def is_duplicate_performance_signal(self, symbol: str, side: str, hours: int = 3) -> bool:
        key = f"PERF_{symbol}_{side}"
        if key in self.performance_signals_sent:
            return (datetime.now() - self.performance_signals_sent[key]).total_seconds() < hours * 3600
        return False
 
    def format_flow_signal_message(self, signal_data: Dict[str, Any]) -> Optional[str]:
        try:
            if signal_data['side'] == "LONG":
                emoji, title, action_emoji, reason = "🟢", "سیگنال LONG (خرید)", "📈", "📥 ورود استیبل کوین"
            else:
                emoji, title, action_emoji, reason = "🔴", "سیگنال SHORT (فروش)", "📉", "📤 خروج به استیبل کوین"
            s1 = "⭐" * signal_data['stage1_score']
            s2 = "⭐" * signal_data['stage2_score']
            s3 = "⭐" * signal_data['stage3_score']
            s4 = "⭐" * signal_data['stage4_score']
            return f"""
{emoji} <b>{title}</b> {action_emoji}
<b>مبتنی بر جریان استیبل کوین</b>
 
<b>💰 ارز:</b> <b>{signal_data['name']}</b> ({signal_data['symbol']})
<b>🎯 امتیاز کل:</b> {signal_data['total_score']}/12
<b>📊 دلیل:</b> {reason}
 
<b>📈 مراحل امتیازدهی:</b>
1️⃣ جریان سرمایه: {s1} ({signal_data['stage1_score']}/3) - {format_number(signal_data['net_flow'])}
2️⃣ نسبت حجم/مارکت‌کپ: {s2} ({signal_data['stage2_score']}/3) - {signal_data['volume_ratio']:.2f}%
3️⃣ عملکرد نسبی به BTC: {s3} ({signal_data['stage3_score']}/3) - {format_percent(signal_data['relative_performance'])}
4️⃣ اندازه بازار: {s4} ({signal_data['stage4_score']}/3) - {format_number(signal_data['market_cap'])}
 
<b>🔍 جزئیات فنی:</b>
• 💵 قیمت فعلی: {format_price(signal_data['price'])}
• 💰 مارکت‌کپ: {format_number(signal_data['market_cap'])}
• 📊 حجم 24h: {format_number(signal_data['volume_24h'])}
 
<b>💸 جریان استیبل کوین:</b>
• 📥 ورودی 24h: {format_number(signal_data['inflow'])}
• 📤 خروجی 24h: {format_number(signal_data['outflow'])}
• 🔄 خالص جریان: {format_number(signal_data['net_flow'])}
 
<b>⏰ زمان شناسایی:</b> {signal_data['timestamp']}
 
⚠️ <b>مدیریت ریسک را فراموش نکنید</b>
"""
        except Exception as e:
            logger.error(f"❌ خطا در فرمت‌بندی پیام: {e}")
            return None
 
    def format_performance_signal_message(self, signal_data: Dict[str, Any]) -> Optional[str]:
        try:
            if signal_data.get('side') == "LONG":
                emoji, title, action_emoji, reason = "🟢", "سیگنال LONG (خرید)", "📈", "📈 عملکرد قوی‌تر از BTC"
            else:
                emoji, title, action_emoji, reason = "🔴", "سیگنال SHORT (فروش)", "📉", "📉 عملکرد ضعیف‌تر از BTC"
            s1 = "⭐" * signal_data.get('stage1_score', 0)
            s2 = "⭐" * signal_data.get('stage2_score', 0)
            s3 = "⭐" * signal_data.get('stage3_score', 0)
            s4 = "⭐" * signal_data.get('stage4_score', 0)
            return f"""
{emoji} <b>{title}</b> {action_emoji}
<b>مبتنی بر عملکرد نسبی</b>
 
<b>💰 ارز:</b> {signal_data.get('name')} ({signal_data.get('symbol')})
<b>🎯 امتیاز کل:</b> {signal_data.get('total_score', 0)}/12
<b>📊 دلیل:</b> {reason}
 
<b>📈 مراحل امتیازدهی:</b>
1️⃣ عملکرد نسبی به BTC: {s1} ({signal_data.get('stage1_score', 0)}/3)
2️⃣ نسبت حجم/مارکت‌کپ: {s2} ({signal_data.get('stage2_score', 0)}/3)
3️⃣ رتبه بازار: {s3} ({signal_data.get('stage3_score', 0)}/3)
4️⃣ اندازه بازار: {s4} ({signal_data.get('stage4_score', 0)}/3)
 
<b>🔍 جزئیات فنی:</b>
• 💵 قیمت فعلی: {format_price(signal_data.get('price', 0))}
• 💰 مارکت‌کپ: {format_number(signal_data.get('market_cap', 0))}
• 📊 حجم 24h: {format_number(signal_data.get('volume_24h', 0))}
• 🏆 رتبه بازار: #{signal_data.get('rank', 0)}
 
<b>📈 عملکرد قیمت:</b>
• 📊 تغییرات 24h: {format_percent(signal_data.get('change_24h', 0))}
• ⚡ عملکرد نسبی به BTC: {format_percent(signal_data.get('relative_performance', 0))}
 
<b>⏰ زمان شناسایی:</b> {signal_data.get('timestamp')}
 
⚠️ <b>مدیریت ریسک را فراموش نکنید</b>
"""
        except Exception as e:
            logger.error(f"❌ خطا در فرمت‌بندی پیام عملکردی: {e}")
            return None
 
    def flow_and_performance_scan_job(self):
        try:
            logger.info("🔄 اجرای اسکن جریان استیبل‌کوین + عملکرد نسبی...")
            sent_count = 0
 
            for signal in self.scan_for_flow_signals():
                message = self.format_flow_signal_message(signal)
                if message and self.send_telegram_message(message):
                    logger.info(f"📤 سیگنال جریانی {signal['symbol']} ({signal['side']}) ارسال شد")
                    sent_count += 1
                    time.sleep(2)
 
            performance_signals = self.get_performance_based_signals()
            for signal in performance_signals:
                try:
                    coin_details = self.get_coin_detailed_data([signal['symbol']])
                    if signal['symbol'] in coin_details:
                        d = coin_details[signal['symbol']]
                        signal['volume_ratio'] = d.get('volume_ratio', 0)
                        signal['price'] = d.get('price', signal.get('price', 0))
                        signal['volume_24h'] = d.get('volume_24h', signal.get('volume_24h', 0))
                        signal['market_cap'] = d.get('market_cap', signal.get('market_cap', 0))
                    self.calculate_total_score(signal)
                except Exception as e:
                    logger.error(f"❌ خطا در پردازش سیگنال عملکردی {signal.get('symbol')}: {e}")
                    continue
 
                if not self.is_duplicate_performance_signal(signal['symbol'], signal['side'], hours=3):
                    message = self.format_performance_signal_message(signal)
                    if message and self.send_telegram_message(message):
                        logger.info(f"📤 سیگنال عملکردی {signal['symbol']} ({signal['side']}) ارسال شد")
                        self.performance_signals_sent[f"PERF_{signal['symbol']}_{signal['side']}"] = datetime.now()
                        sent_count += 1
                        time.sleep(2)
 
            logger.info(f"✅ اسکن جریان/عملکرد کامل شد. {sent_count} سیگنال ارسال شد")
        except Exception as e:
            logger.error(f"❌ خطا در اسکن جریان/عملکرد: {e}")
 
 
# ==================================================================
# اجرای اصلی - یک بار اجرا می‌شود و خارج می‌شود (سازگار با GitHub Actions)
# ==================================================================
def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_BOT_TOKEN یا TELEGRAM_CHANNEL_ID تنظیم نشده - خروج")
        return
 
    state = load_state()
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    logger.info(f"🕐 ساعت فعلی UTC: {now_utc.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"🔍 طول کلیدها (برای عیب‌یابی، بدون فاش‌کردن مقدار): "
                f"CMC={len(COINMARKETCAP_API_KEY)}, LCW={len(LIVECOINWATCH_API_KEY)}, "
                f"نهایی‌TWELVEDATA={len(TWELVEDATA_API_KEY)} "
                f"(خام‌TD_API_KEY={len(os.getenv('TD_API_KEY', ''))}, "
                f"خام‌TWELVEDATA_API_KEY={len(os.getenv('TWELVEDATA_API_KEY', ''))})")
 
    run_flow_perf = FORCE_RUN_ALL or hour in SCAN_SCHEDULE_HOURS
    run_candle = FORCE_RUN_ALL or hour in CANDLE_SCAN_HOURS
 
    bot = TradeiscoolBot(state)
 
    if run_flow_perf:
        if COINMARKETCAP_API_KEY and LIVECOINWATCH_API_KEY:
            bot.flow_and_performance_scan_job()
        else:
            logger.warning("⚠️ COINMARKETCAP_API_KEY یا LIVECOINWATCH_API_KEY تنظیم نشده - اسکن جریان/عملکرد رد شد")
 
    if run_candle:
        candle_signal_scan_job(bot, state)
 
    if not run_flow_perf and not run_candle:
        logger.info("ℹ️ ساعت فعلی با هیچ‌کدام از زمان‌بندی‌ها مطابقت ندارد؛ خروج بدون اسکن")
 
    bot.sync_state()
    save_state(state)
 
 
if __name__ == "__main__":
    main()
 
