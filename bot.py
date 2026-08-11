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


# فرمت مشترک همه‌ی انواع سیگنال (کندلی/جریان) - برای یکسان بودن ظاهر پیام‌ها در کانال
SIGNAL_FOOTER = "⚠️ سیگنال خام - مدیریت ریسک فراموش نشود"


def format_signal_header(emoji: str, side_label: str, name: str, symbol: str, source_label: str) -> str:
    return f"{emoji} <b>{side_label}</b> | {name} ({symbol}) — {source_label}"


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
CANDLE_INTERVAL = "4h"  # برای سازگاری با کدهای قدیمی؛ منبع اصلی تایم‌فریم‌ها حالا TIMEFRAMES است
BOOTSTRAP_LIMIT = 300     # تعداد کندل برای گرم‌کردن اولیه EMA/ATR هنگام اولین اجرا برای هر نماد+تایم‌فریم
COOLDOWN_BARS = 5         # cooldownBars در اسکریپت اصلی (بر اساس تعداد کندل، نه زمان - پس برای همه‌ی تایم‌فریم‌ها یکسان و درست است)
WHIPSAW_ATR_MULT = 0.5    # minMoveATRMultiplier در اسکریپت اصلی
EMA_SLOPE_ATR_MULT = 0.03  # جایگزینِ نسبی و مقیاس‌پذیرِ emaSlopeThreshold ثابتِ اسکریپت اصلی (توضیح در step_candle_state)
CANDLE_BODY_MAX_RATIO = 0.5
SHADOW_RATIO = 3.5

# فیلتر قدرت روند (ADX ۱۴ - روش وایلدر) - برای رفع مشکل «سیگنال کندلی در بازار
# رنج عملکرد خوبی ندارد»: اندیکاتور اصلی صرفاً بر اساس شکل کندل + جهت EMA7/EMA25
# تصمیم می‌گیرد و در بازار رنج (که EMA7/EMA25 هم مدام جهت عوض می‌کنند) سیگنال‌های
# کاذب زیادی تولید می‌کند. ADX قدرت روند را مستقل از جهت آن می‌سنجد؛ زیر آستانه
# یعنی بازار در حال رنج زدن است و سیگنال گرفته نمی‌شود. ۲۰ آستانه‌ی رایج تحلیل
# تکنیکال است (زیر ۲۰ = بدون روند مشخص، بالای ۲۵ = روند قوی).
ADX_TREND_THRESHOLD = 20

# تایم‌فریم‌هایی که سیگنال کندلی برایشان بررسی می‌شود. هر تایم‌فریم به‌اندازه‌ی
# منطقی خودش چک می‌شود (کوتاه‌ترها بیشتر، بلندترها کمتر) تا از سهمیه‌ی روزانه‌ی
# رایگان Twelve Data (۸۰۰ درخواست/روز) عبور نکنیم. منطق دقیق در is_timeframe_due است.
# ۵ دقیقه‌ای عمداً حذف شد: پرمصرف‌ترین تایم‌فریم بود (۲۸۸ درخواست از ۸۰۰ در روز فقط
# برای همین یکی)؛ سهمیه‌اش به رصد لحظه‌ای ارزهای پرتحرک (HOT_MOVERS) اختصاص یافت.
TIMEFRAMES = {
    "15m": {"td_interval": "15min", "bar_seconds": 15 * 60,         "label": "۱۵ دقیقه‌ای"},
    "1h":  {"td_interval": "1h",    "bar_seconds": 60 * 60,         "label": "۱ ساعته"},
    "4h":  {"td_interval": "4h",    "bar_seconds": 4 * 60 * 60,     "label": "۴ ساعته"},
    "1d":  {"td_interval": "1day",  "bar_seconds": 24 * 60 * 60,    "label": "روزانه"},
    "1w":  {"td_interval": "1week", "bar_seconds": 7 * 24 * 60 * 60, "label": "هفتگی"},
}


def _tf_window_id(tf_key: str, now_utc: datetime) -> Optional[str]:
    """
    شناسه‌ی «پنجره‌ی زمانی جاری» برای هر تایم‌فریم (مثلاً برای ۴ساعته: کدام بازه‌ی
    ۴ساعته‌ی امروز). این شناسه در state ذخیره می‌شود تا اگر اجرای ورک‌فلو دیر شروع
    شود (تاخیر زمان‌بندی خودِ GitHub Actions - رایج در دقیقه‌ی صفر هر ساعت)، به‌جای
    از دست رفتن کامل آن پنجره (تا نوبت بعدی، که می‌تواند تا ۴ ساعت/۱ روز/۱ هفته طول
    بکشد)، همچنان تا قبل از شروع پنجره‌ی بعدی این یکی پردازش شود.
    """
    if tf_key == "1h":
        return now_utc.strftime("%Y-%m-%d-%H")
    if tf_key == "4h":
        bucket_hour = max(h for h in CANDLE_SCAN_HOURS if h <= now_utc.hour)
        return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"
    if tf_key == "1d":
        return now_utc.strftime("%Y-%m-%d")
    if tf_key == "1w":
        iso = now_utc.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    return None


def is_timeframe_due(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> bool:
    """
    تعیین می‌کند آیا الان زمانِ چک‌کردن این تایم‌فریم هست یا نه.
    این تابع هر ۱۵ دقیقه یک‌بار (طبق cron ورک‌فلو) صدا زده می‌شود، ولی به‌جای
    تکیه بر ساعت/دقیقه‌ی دیوارساعت (که با تاخیرهای زمان‌بندی GitHub Actions
    می‌تواند کل یک پنجره را از دست بدهد)، بر اساس «آخرین پنجره‌ی پردازش‌شده»
    (ذخیره‌شده در state) تصمیم می‌گیرد - پس حتی اگر اجرا چند دقیقه دیر شروع شود،
    تا وقتی پنجره‌ی بعدی نرسیده، همچنان آن را پردازش می‌کند.
    """
    if FORCE_RUN_ALL:
        return True
    if tf_key == "15m":
        return True                                   # هر اجرا (هر ۱۵ دقیقه)
    window_id = _tf_window_id(tf_key, now_utc)
    last_processed = state.get("tf_windows", {}).get(tf_key)
    return last_processed != window_id


def mark_timeframe_processed(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> None:
    window_id = _tf_window_id(tf_key, now_utc)
    if window_id is None:
        return
    state.setdefault("tf_windows", {})[tf_key] = window_id

# لیست کامل نمادهایی که سیگنال کندلی برایشان بررسی می‌شود (کلید رایگان لازم است:
# https://twelvedata.com). برای اضافه/حذف نماد، همین دیکشنری را ویرایش کنید.
#
# نکته: نقره (XAG/USD) و مس (XCU/USD) عمداً از این لیست حذف شده‌اند. طبق تست مستقیم و
# مستندات Twelve Data، داده‌ی قیمت این دو مورد ("Commodities Market data") فقط در پلن
# پولی Grow (از ۲۹ دلار/ماه) موجوده - پلن رایگان فقط اسم/توضیحات نماد رو می‌ده، نه قیمت
# واقعی. اگر بعداً پلن رو آپگرید کردید یا منبع دیگری پیدا کردید، کافیست این دو خط رو
# دوباره اضافه کنید:
#     "XAG/USD": "نقره (Silver)",
#     "XCU/USD": "مس (Copper)",
WATCHLIST_SYMBOLS = {
    "BTC/USD": "بیت‌کوین (BTC)",
    "ETH/USD": "اتریوم (ETH)",
    "XAU/USD": "طلا (Gold)",
}
# این سه نماد همه‌ی TIMEFRAMES رو دارن (۵د،۱۵د،۱س،۴س،روزانه،هفتگی) - بدون محدودیت.

# ۳۰ ارز برتر بازار (بر اساس مارکت‌کپ، به‌صورت زنده از CoinMarketCap) - طبق درخواست
# کاربر فقط تایم‌فریم ۴ ساعته دارن. دلیل محدودیت فقط-۴-ساعته: با سهمیه‌ی رایگان
# Twelve Data (۸۰۰ درخواست/روز) امکان چک‌کردن ۳۰ نماد در تایم‌فریم ۱۵ دقیقه‌ای با
# فرکانس معنادار وجود ندارد (فقط همین یکی به‌تنهایی ~۲۸۸۰ درخواست/روز می‌شود که
# خیلی بیشتر از کل سهمیه‌ی روزانه‌ست). به همین دلیل این گروه با فرکانس کمتر
# (۴ بار در روز به‌جای هر ۱۵ دقیقه) و فقط روی ۴ ساعته چک می‌شود تا مجموع سهمیه
# زیر ۸۰۰ درخواست در روز بماند و ربات از کار نیفتد.
TOP30_ENABLED = True
TOP30_COUNT = 30
TOP30_TIMEFRAME = "4h"
TOP30_SCAN_HOURS = [0, 6, 12, 18]  # ۴ بار در روز (نه ۶ بار، برای صرفه‌جویی در سهمیه)

# ---------------- ارزهای پرتحرک (برنده/بازنده نسبت به BTC) ----------------
# طبق درخواست: به‌جای امتیازدهی خام روی درصد تغییر (روش قدیمی PERFORMANCE)، همان
# موتور کندلی روی این ارزها هم اجرا می‌شود - سیگنال واقعی فقط وقتی صادر می‌شود که
# اندیکاتور کندلی (پین‌بار/اینگولفینگ + EMA7/EMA25 + فیلتر ADX) روی آن‌ها هم تایید کند.
# لیست هر ۳ ساعت (هم‌زمان با اسکن جریان استیبل‌کوین) از CoinMarketCap بروز می‌شود.
#
# محدودیت سهمیه‌ی API: چک ۱۲ ارز در تایم‌فریم ۱۵ دقیقه‌ای واقعاً لحظه‌ای (هر ۱۵
# دقیقه) به‌تنهایی ۱۱۵۲ درخواست/روز لازم دارد - بیشتر از کل سهمیه‌ی رایگان Twelve
# Data (۸۰۰/روز). به همین دلیل ۱۵ دقیقه‌ای این گروه هر ۲ ساعت چک می‌شود؛ چون فقط
# آخرین ۱۰ کندل بسته‌شده خوانده می‌شود هیچ کندلی از قلم نمی‌افتد، فقط ممکن است
# کشف سیگنال تا ۲ ساعت بعد از تشکیل واقعی‌اش رخ دهد (نه لحظه‌به‌لحظه، ولی خیلی
# نزدیک‌تر از حالت قبلی که اصلاً روی این ارزها کندل چک نمی‌شد). تایم‌فریم ۴ ساعته‌
# این گروه هم‌زمان با واچ‌لیست اصلی و کاملاً لحظه‌ای (هم‌راستا با CANDLE_SCAN_HOURS) چک می‌شود.
HOT_MOVERS_COUNT_EACH = 6   # ۶ برنده + ۶ بازنده = ۱۲ ارز
HOT_MOVERS_TIMEFRAMES = ["4h", "15m"]
HOT_MOVERS_15M_SCAN_HOURS = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]  # هر ۲ ساعت

TWELVEDATA_BASE = "https://api.twelvedata.com"


def _flow_perf_window_id(now_utc: datetime) -> str:
    bucket_hour = max(h for h in SCAN_SCHEDULE_HOURS if h <= now_utc.hour)
    return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"


def is_flow_perf_due(now_utc: datetime, state: Dict[str, Any]) -> bool:
    """مثل is_timeframe_due: بر اساس پنجره‌ی زمانی به‌جای ساعت/دقیقه‌ی دقیق دیوارساعت،
    تا تاخیر زمان‌بندی GitHub Actions باعث از دست رفتن کامل یک دور اسکن نشود."""
    if FORCE_RUN_ALL:
        return True
    last_processed = state.get("tf_windows", {}).get("flow_perf")
    return last_processed != _flow_perf_window_id(now_utc)


def mark_flow_perf_processed(now_utc: datetime, state: Dict[str, Any]) -> None:
    state.setdefault("tf_windows", {})["flow_perf"] = _flow_perf_window_id(now_utc)


def _top30_window_id(now_utc: datetime) -> str:
    bucket_hour = max(h for h in TOP30_SCAN_HOURS if h <= now_utc.hour)
    return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"


def is_top30_due(now_utc: datetime, state: Dict[str, Any]) -> bool:
    if FORCE_RUN_ALL:
        return True
    last_processed = state.get("tf_windows", {}).get("top30")
    return last_processed != _top30_window_id(now_utc)


def mark_top30_processed(now_utc: datetime, state: Dict[str, Any]) -> None:
    state.setdefault("tf_windows", {})["top30"] = _top30_window_id(now_utc)


def _hot_movers_window_id(tf_key: str, now_utc: datetime) -> str:
    if tf_key == "4h":
        return _tf_window_id("4h", now_utc)
    # 15m برای ارزهای پرتحرک: هر ۲ ساعت (به‌خاطر محدودیت سهمیه - توضیح در تعریف ثابت‌ها)
    bucket_hour = max(h for h in HOT_MOVERS_15M_SCAN_HOURS if h <= now_utc.hour)
    return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"


def is_hot_movers_tf_due(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> bool:
    if FORCE_RUN_ALL:
        return True
    window_id = _hot_movers_window_id(tf_key, now_utc)
    last_processed = state.get("tf_windows", {}).get(f"hotmovers_{tf_key}")
    return last_processed != window_id


def mark_hot_movers_tf_processed(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> None:
    window_id = _hot_movers_window_id(tf_key, now_utc)
    state.setdefault("tf_windows", {})[f"hotmovers_{tf_key}"] = window_id


def fetch_closed_klines_twelvedata(symbol: str, limit: int, interval: str, bar_seconds: int) -> List[Dict[str, Any]]:
    """کندل‌های بسته‌شده برای هر نماد و هر تایم‌فریمی از Twelve Data"""
    if not TWELVEDATA_API_KEY:
        return []
    url = f"{TWELVEDATA_BASE}/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": limit,
        "timezone": "UTC",
        "apikey": TWELVEDATA_API_KEY,
    }
    data = retry_request("GET", url, params=params)
    if not data or not isinstance(data, dict):
        return []
    if data.get("status") == "error":
        logger.warning(f"⚠️ Twelve Data برای {symbol} ({interval}) خطا داد: {data.get('message')} "
                        f"(احتمالاً این نماد/تایم‌فریم نیاز به پلن پولی دارد)")
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
            # اگر کندل هنوز کامل نشده رد شود
            if dt.timestamp() + bar_seconds > now:
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
        "plus_dm": None, "minus_dm": None, "dm_buffer": [],
        "adx": None, "dx_buffer": [],
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


def _ensure_candle_state_fields(state: Dict[str, Any]) -> Dict[str, Any]:
    """سازگاری با state.json‌های قدیمی (قبل از اضافه‌شدن فیلتر ADX) که فیلدهای
    جدید را ندارند - جای‌گذاری مقادیر پیش‌فرض بدون از دست دادن پیشرفت موجود
    (EMA/ATR/تاریخچه) نماد."""
    merged = new_candle_state()
    merged.update(state)
    return merged


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
    s["dm_buffer"] = list(s.get("dm_buffer", []))
    s["dx_buffer"] = list(s.get("dx_buffer", []))

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

    # +DM/-DM و ADX(14) - Wilder: فیلتر قدرت روند برای رد سیگنال در بازار رنج
    if prev_bar is not None:
        up_move = h - prev_bar["h"]
        down_move = prev_bar["l"] - l
        plus_dm_raw = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm_raw = down_move if (down_move > up_move and down_move > 0) else 0.0
    else:
        plus_dm_raw = minus_dm_raw = 0.0

    if s.get("plus_dm") is None:
        s["dm_buffer"].append((plus_dm_raw, minus_dm_raw))
        if len(s["dm_buffer"]) >= 14:
            last14 = s["dm_buffer"][-14:]
            plus_dm_smoothed = sum(x[0] for x in last14) / 14.0
            minus_dm_smoothed = sum(x[1] for x in last14) / 14.0
        else:
            plus_dm_smoothed = minus_dm_smoothed = None
    else:
        plus_dm_smoothed = (s["plus_dm"] * 13 + plus_dm_raw) / 14.0
        minus_dm_smoothed = (s["minus_dm"] * 13 + minus_dm_raw) / 14.0

    adx_i = s.get("adx")
    if atr_i is not None and atr_i > 0 and plus_dm_smoothed is not None and minus_dm_smoothed is not None:
        plus_di = 100.0 * plus_dm_smoothed / atr_i
        minus_di = 100.0 * minus_dm_smoothed / atr_i
        di_sum = plus_di + minus_di
        dx = 100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0
        if s.get("adx") is None:
            s["dx_buffer"].append(dx)
            adx_i = sum(s["dx_buffer"][-14:]) / 14.0 if len(s["dx_buffer"]) >= 14 else None
        else:
            adx_i = (s["adx"] * 13 + dx) / 14.0
    # تا وقتی ADX کامل نشده (دوره‌ی گرم‌کردن)، محافظه‌کارانه سیگنال مسدود می‌شود
    # (هم‌راستا با رفتار is_ema7_flat وقتی atr_i هنوز None است)
    is_trending = (adx_i is not None) and (adx_i >= ADX_TREND_THRESHOLD)

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

    raw_bull = is_uptrend and is_valid_bull_candle and (not next_invalidates_bull) and (not is_ema7_flat) and both_above and is_trending
    raw_bear = is_downtrend and is_valid_bear_candle and (not next_invalidates_bear) and (not is_ema7_flat) and both_below and is_trending

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
    s["plus_dm"] = plus_dm_smoothed
    s["minus_dm"] = minus_dm_smoothed
    s["adx"] = adx_i
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
        state = _ensure_candle_state_fields(sym_state)
        last_open_time = state.get("last_open_time")
        candles = fetch_fn(symbol, 10)
        new_candles = [k for k in candles if last_open_time is None or k["open_time"] > last_open_time]
        signals = []
        for k in new_candles:
            state, sig = step_candle_state(state, k["o"], k["h"], k["l"], k["c"], k["open_time"])
            if sig:
                signals.append(sig)
        return state, signals


def format_candle_signal_message(symbol: str, sig: Dict[str, Any], display_name: Optional[str] = None,
                                  timeframe_label: str = "۴ ساعته", bar_seconds: int = 0,
                                  source_label: Optional[str] = None, extra_line: Optional[str] = None) -> str:
    side = sig["side"]
    confirmed = sig["confirmed"]
    price = sig["price"]
    # نکته مهم: open_time زمانِ «شروع» کندل سیگنال است، نه زمانی که سیگنال واقعاً
    # تشکیل شد (که فقط با بسته‌شدن/close کندل قطعی می‌شود). قبلاً همین open_time
    # به‌عنوان «زمان کندل» نمایش داده می‌شد که باعث می‌شد پیام - با اینکه دقیقاً سر
    # وقتِ بسته‌شدن کندل ارسال شده بود - در ظاهر یک بازه‌ی کامل (مثلاً ۴ ساعت) دیرتر
    # از زمان واقعی ارسال به‌نظر برسد. اینجا زمان بسته‌شدن/تشکیل واقعی سیگنال نمایش
    # داده می‌شود.
    formed_ts = datetime.fromtimestamp(sig["open_time"] / 1000 + bar_seconds, tz=timezone.utc)
    ts = formed_ts.strftime("%Y-%m-%d %H:%M UTC")
    emoji = "🟢" if side == "BUY" else "🔴"
    side_label = "خرید" if side == "BUY" else "فروش"
    confirm_txt = "✅ تأییدشده (اینگولفینگ/پین‌بار)" if confirmed else "بدون تأیید اضافه"
    clean_symbol = display_name if display_name else (symbol[:-4] if symbol.endswith("USDT") else symbol)

    desc = f"کندلی {timeframe_label}"
    if source_label:
        desc += f" - {source_label}"
    header = format_signal_header(emoji, side_label, clean_symbol, symbol, desc)

    body = f"قیمت: {format_price(price)}\nتایید: {confirm_txt}"
    if extra_line:
        body += f"\n{extra_line}"
    body += f"\nزمان تشکیل: {ts}"

    return f"{header}\n\n{body}\n\n{SIGNAL_FOOTER}"


def candle_signal_scan_job(bot: "TradeiscoolBot", state: Dict[str, Any], now_utc: datetime) -> List[Dict[str, Any]]:
    """
    اسکن سیگنال کندلی برای واچ‌لیست × همه‌ی تایم‌فریم‌های فعال (فقط آن‌هایی که
    is_timeframe_due همین الان لازمشان بداند). برای هرکدام از ترکیب‌های
    نماد+تایم‌فریم یک گزارش وضعیت برمی‌گرداند (موفق/ناموفق/سیگنال/دلیل سکوت).
    """
    candle_states = state.setdefault("candle_signals", {})
    sent_count = 0
    report: List[Dict[str, Any]] = []
    processed_this_run = set()  # پرهیز از فچ/پردازش تکراری وقتی یک نماد در چند گروه (مثلاً هم ۳۰-برتر هم پرتحرک) باشد

    def _describe_silence(new_state: Dict[str, Any]) -> str:
        """چرا الان سیگنالی نیامد - برای گزارش تشخیصی"""
        if not new_state:
            return "داده کافی نیست"
        trend = new_state.get("trend_prev")
        trend_fa = "صعودی" if trend == "up" else ("نزولی" if trend == "down" else "خنثی")
        adx = new_state.get("adx")
        if adx is not None and adx < ADX_TREND_THRESHOLD:
            return f"بازار رنج/بدون روند مشخص (ADX={adx:.1f} زیر آستانه‌ی {ADX_TREND_THRESHOLD}) - سیگنال گرفته نمی‌شود"
        if trend == "up" and new_state.get("bull_used_this_trend"):
            return f"روند {trend_fa} - سیگنال این روند قبلاً ارسال شده (تا تغییر روند، سیگنال جدید نمی‌آید)"
        if trend == "down" and new_state.get("bear_used_this_trend"):
            return f"روند {trend_fa} - سیگنال این روند قبلاً ارسال شده (تا تغییر روند، سیگنال جدید نمی‌آید)"
        return f"روند فعلی: {trend_fa} - شکل کندل اخیر با شرایط ورود اندیکاتور مطابقت نداشت"

    # نکته: تایم‌فریم‌ها به‌ترتیب کوتاه‌به‌بلند مرتب می‌شوند و حلقه‌ی بیرونی روی
    # «تایم‌فریم» است نه «نماد» (تایم‌فریم-محور به‌جای نماد-محور). این‌طور برای هر
    # تایم‌فریم، هر سه نماد (BTC/ETH/XAU) پشت‌سرهم و در کوتاه‌ترین فاصله‌ی ممکن از
    # هم چک می‌شوند - به‌جای اینکه مثلاً چک XAU برای همان کندل به‌خاطر چک‌شدن چند
    # تایم‌فریم دیگر برای BTC/ETH قبل از آن، چند ده ثانیه عقب بیفتد.
    tf_order = sorted(TIMEFRAMES.keys(), key=lambda k: TIMEFRAMES[k]["bar_seconds"])
    due_timeframes = [tf for tf in tf_order if is_timeframe_due(tf, now_utc, state)]
    logger.info(f"🕯️ شروع اسکن سیگنال‌های کندلی - تایم‌فریم‌های موعدشده الان: {', '.join(due_timeframes) or '(هیچ‌کدام)'}")

    if not due_timeframes:
        return report

    if not TWELVEDATA_API_KEY:
        logger.warning("⚠️ TWELVEDATA_API_KEY تنظیم نشده - کل اسکن کندلی رد شد")
        for tf in due_timeframes:
            for symbol, display_name in WATCHLIST_SYMBOLS.items():
                report.append({"symbol": symbol, "timeframe": tf, "display_name": display_name, "ok": False,
                                "signal": None, "error": "TWELVEDATA_API_KEY تنظیم نشده", "note": None})
        return report

    total_combos = len(WATCHLIST_SYMBOLS) * len(due_timeframes)
    logger.info(f"🔎 در حال بررسی {len(due_timeframes)} تایم‌فریم × {len(WATCHLIST_SYMBOLS)} نماد = {total_combos} مورد از Twelve Data...")

    for tf in due_timeframes:
        tf_cfg = TIMEFRAMES[tf]
        for symbol, display_name in WATCHLIST_SYMBOLS.items():
            state_key = f"{symbol}|{tf}"
            entry = {"symbol": symbol, "timeframe": tf, "display_name": display_name, "ok": False,
                      "signal": None, "error": None, "note": None}
            fetch_fn = lambda sym, lim, _iv=tf_cfg["td_interval"], _bs=tf_cfg["bar_seconds"]: \
                fetch_closed_klines_twelvedata(sym, lim, _iv, _bs)
            try:
                sym_state = candle_states.get(state_key)
                new_state, signals = process_symbol_candles(fetch_fn, symbol, sym_state)
                if new_state is None:
                    entry["error"] = "دریافت داده از Twelve Data ناموفق بود (ممکن است این نماد/تایم‌فریم نیاز به پلن پولی داشته باشد)"
                else:
                    candle_states[state_key] = new_state
                    entry["ok"] = True
                    entry["note"] = _describe_silence(new_state)
                    processed_this_run.add(state_key)
                    for sig in signals:
                        message = format_candle_signal_message(symbol, sig, display_name=display_name,
                                                                timeframe_label=tf_cfg["label"],
                                                                bar_seconds=tf_cfg["bar_seconds"])
                        if bot.send_telegram_message(message):
                            logger.info(f"📤 سیگنال کندلی {symbol} [{tf}] ({sig['side']}) ارسال شد")
                            sent_count += 1
                            entry["signal"] = sig["side"]
                            time.sleep(1.2)
                time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data (۸ درخواست در دقیقه در پلن رایگان)
            except Exception as e:
                entry["error"] = str(e)
                logger.warning(f"⚠️ خطا در پردازش {symbol} [{tf}]: {e}")
            report.append(entry)
        # این تایم‌فریم برای همه‌ی نمادهای واچ‌لیست چک شد - پنجره را ثبت کن تا در
        # اجراهای بعدی همین پنجره دوباره چک نشود (تا پنجره‌ی بعدی برسد)
        mark_timeframe_processed(tf, now_utc, state)

    # ---------- گروه دوم: ۳۰ ارز برتر بازار بر اساس مارکت‌کپ (فقط ۴ ساعته) ----------
    if TOP30_ENABLED and is_top30_due(now_utc, state):
        if not TWELVEDATA_API_KEY:
            logger.warning("⚠️ TWELVEDATA_API_KEY تنظیم نشده - اسکن ۳۰ ارز برتر رد شد")
        else:
            top_coins = bot.get_top_coins_from_cmc(TOP30_COUNT + 5)  # چند تای اضافه برای اطمینان بعد از فیلتر
            excluded = {"BTC", "ETH"}  # این دو از قبل با تمام تایم‌فریم‌ها در واچ‌لیست اصلی هستند
            top30_symbols = []
            for c in top_coins:
                sym = c.get("symbol", "")
                if sym and sym not in excluded and sym not in [s for s in top30_symbols]:
                    top30_symbols.append(sym)
                if len(top30_symbols) >= TOP30_COUNT:
                    break

            if not top30_symbols:
                logger.warning("⚠️ دریافت لیست ۳۰ ارز برتر از CoinMarketCap ناموفق بود - این بخش رد شد")
            else:
                tf_cfg = TIMEFRAMES[TOP30_TIMEFRAME]
                logger.info(f"🔎 در حال بررسی {len(top30_symbols)} ارز برتر بازار (فقط تایم‌فریم {tf_cfg['label']}) از Twelve Data...")
                fetch_fn = lambda sym, lim, _iv=tf_cfg["td_interval"], _bs=tf_cfg["bar_seconds"]: \
                    fetch_closed_klines_twelvedata(sym, lim, _iv, _bs)
                for cmc_symbol in top30_symbols:
                    td_symbol = f"{cmc_symbol}/USD"
                    state_key = f"{td_symbol}|{TOP30_TIMEFRAME}"
                    if state_key in processed_this_run:
                        continue  # همین نماد+تایم‌فریم قبلاً در همین اجرا پردازش شده
                    entry = {"symbol": td_symbol, "timeframe": TOP30_TIMEFRAME, "display_name": cmc_symbol, "ok": False,
                              "signal": None, "error": None, "note": None}
                    try:
                        sym_state = candle_states.get(state_key)
                        new_state, signals = process_symbol_candles(fetch_fn, td_symbol, sym_state)
                        if new_state is None:
                            entry["error"] = "دریافت داده از Twelve Data ناموفق بود (ممکن است این ارز پشتیبانی نشود)"
                        else:
                            candle_states[state_key] = new_state
                            entry["ok"] = True
                            entry["note"] = _describe_silence(new_state)
                            processed_this_run.add(state_key)
                            for sig in signals:
                                message = format_candle_signal_message(td_symbol, sig, display_name=cmc_symbol,
                                                                        timeframe_label=tf_cfg["label"],
                                                                        bar_seconds=tf_cfg["bar_seconds"],
                                                                        source_label="۳۰ ارز برتر بازار")
                                if bot.send_telegram_message(message):
                                    logger.info(f"📤 سیگنال کندلی {td_symbol} [{TOP30_TIMEFRAME}] ({sig['side']}) ارسال شد")
                                    sent_count += 1
                                    entry["signal"] = sig["side"]
                                    time.sleep(1.2)
                        time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data
                    except Exception as e:
                        entry["error"] = str(e)
                        logger.warning(f"⚠️ خطا در پردازش {td_symbol}: {e}")
                    report.append(entry)
                mark_top30_processed(now_utc, state)

    # ---------- گروه سوم: ارزهای پرتحرک (برنده/بازنده نسبت به BTC) ----------
    # طبق درخواست: سیگنال «عملکردی» دیگر بر اساس امتیاز خام درصد تغییر نیست؛ همان
    # موتور کندلی روی این ارزها هم اجرا می‌شود و فقط با تایید کندلی سیگنال ارسال می‌شود.
    hot_movers = state.get("hot_movers", {}).get("symbols", [])
    if hot_movers:
        for tf in HOT_MOVERS_TIMEFRAMES:
            if not is_hot_movers_tf_due(tf, now_utc, state):
                continue
            tf_cfg = TIMEFRAMES[tf]
            logger.info(f"🔥 در حال بررسی {len(hot_movers)} ارز پرتحرک (تایم‌فریم {tf_cfg['label']}) از Twelve Data...")
            fetch_fn = lambda sym, lim, _iv=tf_cfg["td_interval"], _bs=tf_cfg["bar_seconds"]: \
                fetch_closed_klines_twelvedata(sym, lim, _iv, _bs)
            for mover in hot_movers:
                cmc_symbol = mover["symbol"]
                td_symbol = f"{cmc_symbol}/USD"
                state_key = f"{td_symbol}|{tf}"
                if state_key in processed_this_run:
                    continue
                entry = {"symbol": td_symbol, "timeframe": tf, "display_name": cmc_symbol, "ok": False,
                          "signal": None, "error": None, "note": None}
                try:
                    sym_state = candle_states.get(state_key)
                    new_state, signals = process_symbol_candles(fetch_fn, td_symbol, sym_state)
                    if new_state is None:
                        entry["error"] = "دریافت داده از Twelve Data ناموفق بود (ممکن است این ارز پشتیبانی نشود)"
                    else:
                        candle_states[state_key] = new_state
                        entry["ok"] = True
                        entry["note"] = _describe_silence(new_state)
                        processed_this_run.add(state_key)
                        for sig in signals:
                            perf_line = f"عملکرد نسبی به BTC (در زمان انتخاب): {format_percent(mover['relative_performance'])}"
                            message = format_candle_signal_message(
                                td_symbol, sig, display_name=cmc_symbol, timeframe_label=tf_cfg["label"],
                                bar_seconds=tf_cfg["bar_seconds"],
                                source_label=f"ارز پرتحرک ({mover['reason']})", extra_line=perf_line)
                            if bot.send_telegram_message(message):
                                logger.info(f"📤 سیگنال کندلی {td_symbol} [{tf}] ({sig['side']}) - ارز پرتحرک - ارسال شد")
                                sent_count += 1
                                entry["signal"] = sig["side"]
                                time.sleep(1.2)
                    time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data
                except Exception as e:
                    entry["error"] = str(e)
                    logger.warning(f"⚠️ خطا در پردازش {td_symbol} [{tf}] (پرتحرک): {e}")
                report.append(entry)
            mark_hot_movers_tf_processed(tf, now_utc, state)

    logger.info("——— گزارش تشخیصی اسکن کندلی ———")
    for e in report:
        tf_label = TIMEFRAMES.get(e["timeframe"], {}).get("label", e["timeframe"])
        if e["signal"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ✅ سیگنال {e['signal']} ارسال شد")
        elif e["error"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ❌ خطا - {e['error']}")
        elif e["ok"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ⚪ بدون سیگنال جدید - {e['note']}")
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
        self.crypto_fetcher = AdvancedCryptometerFetcher(self.session, "https://cryptometer.io")

    def sync_state(self):
        """نتایج در حافظه را قبل از ذخیره‌ی نهایی به دیکشنری state برمی‌گرداند"""
        self._state_ref["flow_signals_sent"] = {k: v.isoformat() for k, v in self.flow_signals_sent.items()}

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

    # ---------------- ارزهای پرتحرک (برنده/بازنده نسبت به BTC) ----------------
    def get_hot_mover_symbols(self, count_each: int = 6) -> Dict[str, List[Dict[str, Any]]]:
        """
        به‌جای تولید مستقیم سیگنال بر اساس امتیازِ درصد تغییرِ خام (روش قبلی
        PERFORMANCE)، فقط لیست کاندیدهای پرتحرک (برنده/بازنده نسبت به BTC در ۲۴
        ساعت گذشته) را برمی‌گرداند. سیگنال واقعی برای این ارزها را حالا موتور
        کندلی (همان اندیکاتور پین‌بار/اینگولفینگ + EMA7/EMA25 + فیلتر ADX) صادر
        می‌کند - طبق درخواست: سیگنال عملکردی = سیگنال کندلی روی ارزهای پرتحرک.
        """
        try:
            top_coins = self.get_top_coins_from_cmc(150)
            if not top_coins:
                return {'gainers': [], 'losers': []}
            btc_data = next((c for c in top_coins if c['symbol'] == 'BTC'), None)
            if not btc_data:
                return {'gainers': [], 'losers': []}
            btc_change = safe_get(btc_data, "quote", "USD", "percent_change_24h") or 0

            performance_data = []
            for coin in top_coins:
                try:
                    symbol = coin['symbol']
                    if symbol in STABLECOINS:
                        continue
                    quote_data = safe_get(coin, "quote", "USD", default={})
                    volume_24h = quote_data.get('volume_24h', 0) or 0
                    if volume_24h < 10_000_000:  # فیلتر نقدشوندگی
                        continue
                    coin_change = safe_get(coin, "quote", "USD", "percent_change_24h") or 0
                    performance_data.append({
                        'symbol': symbol, 'name': coin.get('name', ''),
                        'relative_performance': coin_change - btc_change,
                        'rank': coin.get('cmc_rank', 0),
                    })
                except Exception:
                    continue

            gainers = sorted(performance_data, key=lambda x: x['relative_performance'], reverse=True)[:count_each]
            losers = sorted(performance_data, key=lambda x: x['relative_performance'])[:count_each]
            return {'gainers': gainers, 'losers': losers}
        except Exception as e:
            logger.error(f"❌ خطا در دریافت لیست ارزهای پرتحرک: {e}")
            return {'gainers': [], 'losers': []}

    def refresh_hot_movers(self, state: Dict[str, Any]) -> None:
        movers = self.get_hot_mover_symbols(HOT_MOVERS_COUNT_EACH)
        all_movers = (
            [{**c, 'reason': 'برنده نسبت به BTC'} for c in movers['gainers']] +
            [{**c, 'reason': 'بازنده نسبت به BTC'} for c in movers['losers']]
        )
        if all_movers:
            state['hot_movers'] = {
                'symbols': all_movers,
                'updated': datetime.now(timezone.utc).isoformat(),
            }
            logger.info(f"🔥 لیست ارزهای پرتحرک بروزرسانی شد: "
                        f"{', '.join(c['symbol'] for c in movers['gainers'])} (برنده) | "
                        f"{', '.join(c['symbol'] for c in movers['losers'])} (بازنده)")
        else:
            logger.warning("⚠️ دریافت لیست ارزهای پرتحرک ناموفق بود - لیست قبلی (در صورت وجود) نگه داشته می‌شود")

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

    def calculate_market_cap_score(self, market_cap: float) -> int:
        if market_cap >= 10_000_000_000: return 3
        if market_cap >= 1_000_000_000: return 2
        if market_cap >= 100_000_000: return 1
        return 0

    def calculate_total_score(self, signal_data: Dict[str, Any]) -> int:
        """این امتیازدهی فقط برای سیگنال‌های جریان استیبل‌کوین (FLOW) استفاده می‌شود.
        سیگنال «عملکردی» قبلی که بر همین مبنا امتیازدهی می‌شد، حذف و با سیگنال
        مستقیم موتور کندلی روی ارزهای پرتحرک جایگزین شده است."""
        stage1 = self.calculate_flow_score(signal_data['net_flow'])
        stage2 = self.calculate_volume_ratio_score(signal_data.get('volume_ratio', 0))
        stage3 = self.calculate_relative_performance_score(signal_data.get('relative_performance', 0), signal_data['side'])
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

    def format_flow_signal_message(self, signal_data: Dict[str, Any]) -> Optional[str]:
        try:
            emoji = "🟢" if signal_data['side'] == "LONG" else "🔴"
            header = format_signal_header(emoji, signal_data['side'], signal_data['name'],
                                           signal_data['symbol'], "جریان استیبل‌کوین")
            body = (
                f"امتیاز: {signal_data['total_score']}/12\n"
                f"قیمت: {format_price(signal_data['price'])} | مارکت‌کپ: {format_number(signal_data['market_cap'])}\n"
                f"خالص جریان: {format_number(signal_data['net_flow'])} "
                f"(ورودی {format_number(signal_data['inflow'])} / خروجی {format_number(signal_data['outflow'])})\n"
                f"عملکرد نسبی به BTC: {format_percent(signal_data['relative_performance'])}"
            )
            return f"{header}\n\n{body}\n\n⏰ {signal_data['timestamp']}\n{SIGNAL_FOOTER}"
        except Exception as e:
            logger.error(f"❌ خطا در فرمت‌بندی پیام: {e}")
            return None

    def flow_and_performance_scan_job(self):
        try:
            logger.info("🔄 اجرای اسکن جریان استیبل‌کوین...")
            sent_count = 0

            for signal in self.scan_for_flow_signals():
                message = self.format_flow_signal_message(signal)
                if message and self.send_telegram_message(message):
                    logger.info(f"📤 سیگنال جریانی {signal['symbol']} ({signal['side']}) ارسال شد")
                    sent_count += 1
                    time.sleep(2)

            logger.info(f"✅ اسکن جریان استیبل‌کوین کامل شد. {sent_count} سیگنال ارسال شد")
        except Exception as e:
            logger.error(f"❌ خطا در اسکن جریان: {e}")


# ==================================================================
# اجرای اصلی - یک بار اجرا می‌شود و خارج می‌شود (سازگار با GitHub Actions)
# ==================================================================
def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_BOT_TOKEN یا TELEGRAM_CHANNEL_ID تنظیم نشده - خروج")
        return

    state = load_state()
    now_utc = datetime.now(timezone.utc)
    logger.info(f"🕐 ساعت فعلی UTC: {now_utc.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"🔍 طول کلیدها (برای عیب‌یابی، بدون فاش‌کردن مقدار): "
                f"CMC={len(COINMARKETCAP_API_KEY)}, LCW={len(LIVECOINWATCH_API_KEY)}, "
                f"نهایی‌TWELVEDATA={len(TWELVEDATA_API_KEY)} "
                f"(خام‌TD_API_KEY={len(os.getenv('TD_API_KEY', ''))}, "
                f"خام‌TWELVEDATA_API_KEY={len(os.getenv('TWELVEDATA_API_KEY', ''))})")

    run_flow_perf = FORCE_RUN_ALL or is_flow_perf_due(now_utc, state)

    bot = TradeiscoolBot(state)

    if run_flow_perf:
        if COINMARKETCAP_API_KEY and LIVECOINWATCH_API_KEY:
            bot.flow_and_performance_scan_job()
        else:
            logger.warning("⚠️ COINMARKETCAP_API_KEY یا LIVECOINWATCH_API_KEY تنظیم نشده - اسکن جریان رد شد")
        if COINMARKETCAP_API_KEY:
            bot.refresh_hot_movers(state)
        else:
            logger.warning("⚠️ COINMARKETCAP_API_KEY تنظیم نشده - بروزرسانی لیست ارزهای پرتحرک رد شد")
        mark_flow_perf_processed(now_utc, state)
    else:
        logger.info("ℹ️ ساعت/دقیقه‌ی فعلی با زمان‌بندی اسکن جریان/عملکرد مطابقت ندارد - رد شد")

    candle_signal_scan_job(bot, state, now_utc)

    bot.sync_state()
    save_state(state)


if __name__ == "__main__":
    main()
