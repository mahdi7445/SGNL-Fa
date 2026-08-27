#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tradeiscool Bot - نسخه یکپارچه ۵.۰ (اختصاصی آلت‌کوین / تایم‌فریم پایین)
====================================
سه نوع سیگنال را در یک ربات تلگرام ادغام می‌کند:
  1) سیگنال جریان استیبل‌کوین (Cryptometer)               -> هر ۳ ساعت
  2) سیگنال عملکرد نسبی به بیت‌کوین (CoinMarketCap)         -> هر ۳ ساعت
  3) سیگنال کندلی تایم‌فریم پایین (اندیکاتور Pine Script)    -> ۱د/۵د/۱۵د/۳۰د/۱ساعته
     (ترجمه‌ی دقیق منطق اندیکاتور به پایتون + داده‌ی کندل از Twelve Data)

طبق درخواست کاربر: این کانال کاملاً اختصاصی آلت‌کوین‌هاست. بیت‌کوین و اتریوم
به‌طور کامل از لیست نمادهایی که سیگنال ورود/خروج برایشان صادر می‌شود حذف شده‌اند
(هم از واچ‌لیست اصلی، هم از ۳۰ ارز برتر، هم از ارزهای پرتحرک) - کل سهمیه‌ی
Twelve Data و کل ظرفیت اتصال زنده‌ی WebSocket به آلت‌کوین‌ها اختصاص یافته. تنها
جایی که BTC هنوز دیده می‌شود «عملکرد نسبی به BTC» است که صرفاً یک معیار مقایسه‌ای
(درصد تغییر آلت‌کوین منهای درصد تغییر BTC) است، نه یک سیگنال معاملاتی برای خودِ BTC.

همچنین طبق درخواست کاربر: تمام تایم‌فریم‌های سیگنال‌دهی به زیر ۴ ساعت محدود شدند
(۱ دقیقه تا ۱ ساعت) - تایم‌فریم‌های ۴ساعته و هفتگی که قبلاً وجود داشتند کاملاً
حذف شدند تا هر واحد از سهمیه‌ی روزانه‌ی API صرفِ سرعت/دقت کشفِ سیگنال در همین
بازه‌ی زمانی پایین شود (جایی که آلت‌کوین‌ها بیشترین نوسان کوتاه‌مدت را دارند).

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
import copy
import time
import json
import logging
import threading
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import mplfinance as mpf
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
# 🆕 (تضمین «بدون قطعی») دقیقاً مثل candle_engine.py: اگر خطای بحرانی/غیرمنتظره‌ای در
# حلقه‌ی اصلی رخ بدهد، جدا از لاگ Actions (که معمولاً کسی رصدش نمی‌کند)، مستقیم به
# ادمین(ها) هم در تلگرام پیام هشدار فرستاده می‌شود - تا اگر یک روز واقعاً چیزی خراب
# شد (مثلاً API کلید منقضی، یا یک باگ کاملاً پیش‌بینی‌نشده)، فوراً باخبر شویم به‌جای
# اینکه ساعت‌ها یا روزها بعد متوجه سکوت کانال شویم.
ADMIN_USER_IDS = [x.strip() for x in os.getenv('ADMIN_USER_IDS', '').split(',') if x.strip()]

STABLECOINS = ['USDT', 'USDC', 'FDUSD', 'USD', 'BUSD', 'DAI', 'TUSD', 'USDP', 'USDD']

# زمان‌بندی بر اساس ساعت UTC (هر بار یک دور اسکن اجرا شود، این لیست‌ها چک می‌شوند)
SCAN_SCHEDULE_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]   # جریان استیبل‌کوین + عملکرد نسبی، هر ۳ ساعت
# نکته: ثابتِ قدیمیِ CANDLE_SCAN_HOURS (بازه‌های ۴ساعته) طبق درخواست کاربر حذف شد -
# دیگر هیچ تایم‌فریم ۴ساعته یا بالاتری در این ربات فعال نیست؛ همه‌ی تایم‌فریم‌های
# فعال (۱د/۵د/۱۵د/۳۰د/۱ساعته) با پنجره‌بندی داخلی خودشان در TIMEFRAMES/_tf_window_id
# مدیریت می‌شوند، نه با یک لیست ساعت ثابت.
MAX_SCAN_SECONDS = 240   # سقف زمانی هر «دور اسکن» (نه کل حلقه) تا با دور بعدی تداخل نکند

# ---------------- تنظیمات حلقه‌ی پیوسته (رفع تاخیر سیگنال‌های تایم‌فریم پایین) ----------------
# قبلاً bot.py هر ۱۵ دقیقه توسط cron گیت‌هاب اجرا و خارج می‌شد - که هم به‌خاطر تاخیر
# زمان‌بندی خودِ GitHub Actions (که تضمین‌شده نیست) و هم چون بین دو کندل هیچ رهگیری‌ای
# نبود، سیگنال‌های تایم‌فریم ۱۵ دقیقه‌ای می‌توانستند تا چند دقیقه دیر برسند. حالا (مثل
# candle_engine.py) به‌صورت یک پروسه‌ی پیوسته اجرا می‌شود: یک اتصال WebSocket زنده به
# Coinbase برای واچ‌لیست آلت‌کوین (دقت زیر ۱۰ ثانیه، بدون هزینه‌ی API) + یک حلقه‌ی
# داخلی که هر چند دقیقه یک دور اسکن کامل (کندل جدید در همه‌ی گروه‌ها) انجام می‌دهد.
LOOP_MAX_SECONDS = int(os.getenv("LOOP_MAX_SECONDS") or str(5 * 3600 + 20 * 60))  # ~۵ ساعت و ۲۰ دقیقه
SCAN_CYCLE_INTERVAL_SECONDS = 300   # هر ۵ دقیقه یک دور اسکن کامل (سیگنال جدید در همه‌ی تایم‌فریم‌های زیر ۴ ساعت)
WS_CHECK_INTERVAL_SECONDS = 5       # هر ۵ ثانیه معاملات باز واچ‌لیست آلت‌کوین با آخرین تیک WebSocket چک می‌شوند
GIT_COMMIT_EVERY_SECONDS = 120      # هر ۲ دقیقه تغییرات state.json کامیت می‌شود
# 🆕 طبق بازبینی برنامه‌نویس: این دو ثابت (که قبلاً فقط برای رهگیری REST زنده‌ی طلا
# استفاده می‌شدند و بعد از حذف طلا بلااستفاده مانده بودند) الان توسط
# check_rest_live_trades برای رصد زنده‌ی معاملات باز ۳۰ ارز برتر/ارزهای پرتحرک
# (که WebSocket ندارند) دوباره فعال شدند. مقدارها محافظه‌کارانه انتخاب شدند تا با
# سهمیه‌ی باقی‌مانده‌ی روزانه (بعد از واچ‌لیست اصلی + ۳۰ ارز برتر + ارزهای پرتحرک،
# جزئیات در تعریف WATCHLIST_SYMBOLS) جمع کل از سقف ۸۰۰ درخواست/روز فراتر نرود:
# هر ۳۰ دقیقه حداکثر ۱ معامله چک می‌شود = حداکثر ۴۸ درخواست/روز.
LIVE_CHECK_INTERVAL_SECONDS = 30 * 60
MAX_LIVE_CHECKS_PER_RUN = 1

HTTP_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.5

# کش «نمادهای شناخته‌شده‌ی بدون پشتیبانی» در Twelve Data (۴۰۴ یا خطای دائمی گرفتند).
# بسیاری از کوین‌های کوچک لیست ۳۰-ارز-برتر و ارزهای پرتحرک اصلاً در Twelve Data
# وجود ندارند؛ بدون این کش هر ۱۵ دقیقه دوباره امتحان و رد می‌شدند و وقت/سهمیه تلف
# می‌شد. اینجا برای ۲۴ ساعت به خاطر سپرده می‌شود (نه برای همیشه، چون ممکن است
# بعداً پشتیبانی اضافه شود یا خطا واقعاً موقتی/شبکه‌ای بوده باشد).
TD_UNSUPPORTED_TTL_SECONDS = 24 * 3600
_TD_UNSUPPORTED: Dict[str, float] = {}  # symbol -> unix epoch expiry


def load_td_unsupported_cache(state: Dict[str, Any]) -> None:
    global _TD_UNSUPPORTED
    _TD_UNSUPPORTED = dict(state.get("td_unsupported", {}))


def save_td_unsupported_cache(state: Dict[str, Any]) -> None:
    now = time.time()
    state["td_unsupported"] = {k: v for k, v in _TD_UNSUPPORTED.items() if v > now}

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
    """
    ۴۰۴ (نماد پیدا نشد) یک خطای دائمی است - تلاش مجدد برایش فقط وقت و سهمیه تلف
    می‌کند (و چون تلاش‌های پشت‌سرهم خیلی نزدیک به هم می‌افتند، باعث ۴۲۹ روی
    درخواست‌های بعدی هم می‌شود). این تابع دیگر ۴۰۴/۴۰۱/۴۰۳/۴۰۰/۴۲۲ را تلاش مجدد
    نمی‌کند. ۴۲۹ (محدودیت نرخ) هم به‌جای تلاش سریع، فقط یک‌بار ~۶۵ ثانیه صبر
    می‌کند (پنجره‌ی محدودیت Twelve Data ۶۰ ثانیه‌ای است) و اگر باز هم ۴۲۹ بود، رها می‌کند.
    """
    non_retryable = {400, 401, 403, 404, 422}
    attempt = 0
    retried_429 = False
    while attempt < RETRY_ATTEMPTS:
        attempt += 1
        try:
            resp = requests.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    return resp.text
            if resp.status_code == 429:
                if retried_429:
                    logger.warning(f"HTTP 429 برای {url} - بعد از یک‌بار صبر هم هنوز محدود است، رد می‌شود")
                    return None
                logger.warning(f"HTTP 429 (محدودیت نرخ) برای {url} - ۶۵ ثانیه صبر می‌کنیم (یک‌بار)")
                time.sleep(65)
                retried_429 = True
                continue
            if resp.status_code in non_retryable:
                logger.warning(f"HTTP {resp.status_code} برای {url} (خطای دائمی، تلاش مجدد نمی‌شود)")
                return None
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
SIGNAL_FOOTER = "⚠️ Please manage your risk and capital appropriately."



def format_price(price: float) -> str:
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.6f}"
    else:
        return f"${price:.8f}"


def format_signal_price(price: float) -> str:
    """اعداد سیگنال بدون علامت $، مطابق فرمت رفرنس (Entry: 4397.92). برای نمادهای
    بالای ۱ دلار دقیقاً ۲ رقم اعشار (طبق درخواست). برای رمزارزهای زیر ۱ دلار، چون
    ۲ رقم اعشار ثابت باعث می‌شد قیمت به صفر/بی‌معنی گرد شود (مثلاً یک آلت‌کوین
    ۰.۰۰۰۲۳۴ دلاری می‌شد 0.00)، تعداد رقم اعشار بر اساس بزرگی خودِ قیمت تنظیم
    می‌شود تا همیشه چند رقم بامعنی نشان داده شود."""
    if price >= 1:
        return f"{price:.2f}"
    elif price >= 0.01:
        return f"{price:.4f}"
    elif price >= 0.0001:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"


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
    """طبق بازبینی برنامه‌نویس: ذخیره‌سازی اتمیک - ابتدا در یک فایل موقت نوشته می‌شود
    و فقط بعد از موفقیت کامل، با os.replace (که در سطح سیستم‌عامل اتمیک است) جای
    state.json اصلی می‌نشیند. این یعنی اگر پروسه دقیقاً وسط نوشتن فایل (مثلاً به‌خاطر
    timeout یا kill ناگهانی GitHub Actions) متوقف شود، هیچ‌وقت یک state.json نیمه‌نوشته
    و خراب (JSON نامعتبر) روی دیسک باقی نمی‌ماند - یا نسخه‌ی قبلی کامل سالم است یا
    نسخه‌ی جدید کامل سالم، هیچ حالت بینابینی وجود ندارد."""
    try:
        tmp_path = STATE_FILE + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_FILE)
        logger.info("💾 state.json ذخیره شد (اتمیک)")
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره state.json: {e}")


ADMIN_ALERT_COOLDOWN_SECONDS = 20 * 60  # حداکثر یک هشدار مشابه هر ۲۰ دقیقه (ضدِ اسپم)


def notify_admin(message: str, state: Optional[Dict[str, Any]] = None, kind: str = "generic") -> None:
    """پیام هشدار را مستقیم (جدا از کانال اصلی) به تک‌تک ADMIN_USER_IDS می‌فرستد.
    هرگز نباید خودش استثنا پرتاب کند - این تابع همیشه در دلِ یک except دیگر صدا زده
    می‌شود و نباید مسیر بازیابی از خطا را با یک خطای جدید خراب کند. کول‌دان (بر اساس
    kind) در state ذخیره می‌شود تا هم در همین اجرا و هم در اجراهای بعدی (که state از
    ریموت pull می‌شود) از اسپم‌شدن ادمین با هشدارهای تکراری برای یک مشکل مداوم
    جلوگیری شود."""
    if not ADMIN_USER_IDS or not TELEGRAM_BOT_TOKEN:
        return
    try:
        now_ts = time.time()
        cooldowns = state.setdefault("admin_alert_cooldowns", {}) if state is not None else {}
        last_sent = cooldowns.get(kind, 0)
        if now_ts - last_sent < ADMIN_ALERT_COOLDOWN_SECONDS:
            return
        for admin_id in ADMIN_USER_IDS:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    data={"chat_id": admin_id, "text": f"🚨 [bot.py] {message}"},
                    timeout=15,
                )
            except Exception as e:
                logger.warning(f"⚠️ ارسال هشدار ادمین به {admin_id} ناموفق بود: {e}")
        if state is not None:
            cooldowns[kind] = now_ts
    except Exception as e:
        logger.warning(f"⚠️ notify_admin خودش خطا داد (نادیده گرفته شد): {e}")


def git_commit_and_push():
    """هر چند دقیقه یک‌بار تغییرات state.json را کامیت و پوش می‌کند - چون این اسکریپت
    الان ساعت‌ها پیوسته اجرا می‌ماند، دیگر نمی‌شود فقط منتظر کامیت نهایی workflow ماند
    (اگر پروسه هر جایی متوقف/کش شود، پیشرفت از دست نمی‌رود).

    🔴 رفعِ قطعیِ باگِ «گزارش روزانه بیش از یک‌بار ارسال می‌شود» (و هر رویداد دیگری که
    در state.json علامت‌گذاری می‌شود): علت این بود که اینجا از «git pull --rebase
    -X ours» استفاده می‌شد. برخلاف چیزی که این کامنت قبلاً (اشتباهاً) ادعا می‌کرد،
    معنای ours/theirs در rebase برعکسِ merge معمولی است: در rebase، «ours» به شاخه‌ای
    که رویش rebase می‌شود (یعنی نسخه‌ی ریموت/pull-شده) اشاره دارد، نه نسخه‌ی محلی؛
    «theirs» نسخه‌ی محلی (کامیت‌هایی که دوباره روی آن replay می‌شوند) است. یعنی
    «-X ours» دقیقاً برعکسِ منظور قبلی عمل می‌کرد: هر بار conflict واقعی رخ می‌داد
    (که برای یک فایل JSON یکپارچه مثل state.json تقریباً هر تغییر همزمان conflict
    می‌سازد)، نسخه‌ی محلیِ تازه (که پرچم «گزارش امروز فرستاده شد» تازه در آن ست شده
    بود) بی‌صدا دور ریخته و نسخه‌ی قدیمی‌تر ریموت جایگزین می‌شد؛ چون این پروسه در
    حافظه همچنان فکر می‌کرد پرچم را زده (و به همین دلیل خودش دوباره در همان اجرا
    گزارش را نمی‌فرستاد)، اما آن پرچم هرگز واقعاً روی ریموت نمی‌رسید - اجرای بعدی
    (که state.json را کاملاً تازه از ریموت می‌خواند، نه از دیسک محلیِ اجرای قبلی)
    دوباره due می‌دیدش و دوباره می‌فرستاد. راه‌حل: «-X theirs» (نه ours) - چون در
    این ریپازیتوری این فایل فقط توسط همین پروسه‌ی تک‌نویسنده نوشته می‌شود (صف
    concurrency در signals.yml تضمین می‌کند هیچ‌وقت دو اجرا واقعاً هم‌زمان ننویسند)،
    نسخه‌ی محلی همیشه superset درستِ نسخه‌ی ریموت است و باید همیشه برنده باشد.
    دقیقاً همین باگ (و همین رفع) در signals.yml (مرحله‌ی «Final commit») هم اصلاح شد."""
    try:
        import subprocess
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        # اگر از تلاش قبلی یک rebase نیمه‌کاره باقی مانده، اول آن را پاک کن تا گیر نکنیم
        subprocess.run(["git", "rebase", "--abort"], cwd=repo_dir, check=False)
        subprocess.run(["git", "add", "state.json"], cwd=repo_dir, check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_dir)
        has_local_changes = (result.returncode != 0)
        if has_local_changes:
            subprocess.run(["git", "commit", "-m", "update state [skip ci]"], cwd=repo_dir, check=True)
        # ⚠️ رفع باگ: قبلاً اگر این پروسه خودش هیچ تغییر محلی‌ای برای کامیت نداشت، اصلاً
        # git pull هم اجرا نمی‌شد (return زودهنگام بالا). یعنی اگر یک اجرای موازی دیگر
        # (مثلاً به‌خاطر concurrency صف‌شده‌ی GitHub Actions) همین الان یک پرچم مهم
        # (مثل «گزارش روزانه امروز فرستاده شد») را پوش کرده بود، این پروسه با state
        # محلی/قدیمی خودش کار می‌کرد و همان رویداد را دوباره due می‌دید - دقیقاً همان
        # علتی که باعث تکرار گزارش روزانه می‌شد. الان pull همیشه (چه تغییر محلی باشد
        # چه نه) در همین جا اجرا می‌شود - دقیقاً مثل candle_engine.py.
        pull = subprocess.run(["git", "pull", "--rebase", "-X", "theirs"], cwd=repo_dir, check=False)
        if pull.returncode != 0:
            logger.warning("⚠️ [git] rebase ناموفق بود - لغو و تلاش مجدد برای پوش مستقیم")
            subprocess.run(["git", "rebase", "--abort"], cwd=repo_dir, check=False)

        # 🔴 رفعِ قطعیِ باگِ «گزارش روزانه بیش از یک‌بار ارسال می‌شود» (ریشه‌ی واقعی که در
        # بازبینی کد پیدا شد): قبلاً تصمیم به تلاش برای push فقط بر اساس has_local_changes
        # (یعنی آیا همین فراخوانی چیز *جدیدی* برای کامیت داشت) گرفته می‌شد. اما اگر یک بار
        # کامیت محلی موفق می‌شد ولی push (بعد از هر ۳ تلاش) شکست می‌خورد، آن کامیت برای
        # همیشه فقط محلی می‌ماند: در فراخوانی بعدی (۱۲۰ ثانیه بعد) چون state.json دیگر هیچ
        # تغییر *جدیدی* نسبت به همان کامیتِ پوش‌نشده نداشت، has_local_changes دوباره False
        # می‌شد و کل بلوک push (پایین) اصلاً اجرا نمی‌شد - یعنی پرچم «گزارش امروز فرستاده
        # شد» که فقط محلی کامیت شده بود، هیچ‌وقت واقعاً به ریموت نمی‌رسید و با تمام‌شدن این
        # اجرا (و پاک‌شدن runner) کاملاً از بین می‌رفت. اجرای بعدی (fresh checkout از
        # ریموت) دوباره due می‌دیدش و دوباره می‌فرستاد. راه‌حل: صرف‌نظر از has_local_changes،
        # همیشه چک می‌کنیم آیا اصلاً کامیتی جلوتر از ریموت (unpushed) داریم؛ اگر بله، باید
        # تلاش برای push انجام شود - دقیقاً همین منطق در signals.yml («Final commit») هم
        # اضافه شد.
        try:
            ahead_result = subprocess.run(
                ["git", "rev-list", "--count", "@{u}..HEAD"],
                cwd=repo_dir, capture_output=True, text=True, check=False,
            )
            unpushed_count = int(ahead_result.stdout.strip() or "0") if ahead_result.returncode == 0 else 0
        except Exception:
            unpushed_count = 0
        needs_push = has_local_changes or unpushed_count > 0

        if needs_push:
            if unpushed_count > 0 and not has_local_changes:
                logger.warning(f"⚠️ [git] {unpushed_count} کامیت محلیِ پوش‌نشده از قبل پیدا شد "
                                "(احتمالاً push یک دور قبلی شکست خورده بود) - تلاش دوباره برای push")
            # 🔴 تلاش مجدد برای push: قبلاً یک شکست شبکه‌ای/موقتِ push فقط لاگ می‌شد و
            # فراموش می‌شد - یعنی پرچم «گزارش روزانه فرستاده شد» فقط محلی می‌ماند و اگر
            # دور بعدیِ همین حلقه (هر GIT_COMMIT_EVERY_SECONDS) هم به هر دلیلی push
            # نمی‌کرد، تا پایان همین اجرا هرگز به ریموت نمی‌رسید. الان تا ۳ بار با
            # فاصله‌ی کوتاه دوباره امتحان می‌شود قبل از تسلیم‌شدن.
            push_ok = False
            for attempt in range(3):
                push = subprocess.run(["git", "push"], cwd=repo_dir, check=False)
                if push.returncode == 0:
                    push_ok = True
                    break
                logger.warning(f"⚠️ [git] پوش ناموفق بود (تلاش {attempt + 1}/۳)")
                if attempt < 2:
                    time.sleep(5)
                    # قبل از تلاش دوباره، یک‌بار دیگر pull تا اگر remote عوض شده، همگام شویم
                    subprocess.run(["git", "pull", "--rebase", "-X", "theirs"], cwd=repo_dir, check=False)
            if push_ok:
                logger.info("📌 [git] تغییرات state.json کامیت و پوش شد")
            else:
                logger.error("❌ [git] پوش بعد از ۳ تلاش هم ناموفق بود - علامت‌های state.json "
                              "(از جمله گزارش روزانه، در صورت وجود) فقط محلی ماندند؛ دور بعدی "
                              "این حلقه (که الان دیگر این کامیتِ پوش‌نشده را هم می‌بیند و دوباره "
                              "تلاش می‌کند) این وضعیت را جبران می‌کند")
        elif pull.returncode == 0:
            logger.info("📥 [git] state.json تازه‌ی ریموت pull شد (تغییر محلی‌ای برای پوش نبود)")
    except Exception as e:
        logger.warning(f"⚠️ [git] کامیت/پوش ناموفق بود: {e}")


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
# نکته مهم: در ابتدا از API خودِ Binance استفاده می‌شد، ولی Binance به‌طور رسمی
# دسترسی به API عمومی‌اش را از IP آمریکا مسدود کرده (خطای HTTP 451) و سرورهای
# GitHub Actions همیشه IP آمریکا دارند - این یک محدودیت دائمی از طرف Binance است
# و هیچ راه‌حل کدی ندارد. به همین دلیل همه‌ی نمادها از طریق Twelve Data خوانده
# می‌شوند که یک IP آمریکایی نیست که Binance بلاکش کند و خودش از ۱۸۰+ صرافی/بروکر
# (از جمله Binance) داده جمع می‌کند.
# ==================================================================
BOOTSTRAP_LIMIT = 300     # تعداد کندل برای گرم‌کردن اولیه EMA/ATR هنگام اولین اجرا برای هر نماد+تایم‌فریم
CHART_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chart_tmp.png")
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

# 🆕 فیلتر «روند حجم» - فقط برای تایم‌فریم ۱ دقیقه‌ای (طبق درخواست کاربر): این
# تایم‌فریم به‌خاطر نویز ذاتیِ کندل‌های خیلی کوتاه، سیگنال زیاد و با وین‌ریت پایین
# در هر دو جهت خرید/فروش تولید می‌کرد. دقیقاً همان منطق گروه «فیلتر روند حجم» در
# اسکریپت مرجع Pine Script (volumeTrendUp: میانگین حجم N کندل اخیر > میانگین N
# کندل قبل از آن‌ها) اینجا هم پیاده شده - با این تفاوت که در نسخه‌ی پایتون این
# دیگر شرط اختیاریِ صرفاً «تأییدشده» نیست، بلکه واقعاً سیگنال ۱ دقیقه‌ای را رد
# می‌کند (طبق درخواست صریح). عمداً فقط تایم‌فریم ۱د را تحت تاثیر قرار می‌دهد -
# ۵د/۱۵د/۱ساعته که وین‌ریت مشکلی نداشتند دست‌نخورده می‌مانند.
# طبق درخواست «نباید خیلی سخت باشد»: فقط میانگین ۳ کندل اخیر باید از میانگین ۳
# کندل قبل از آن‌ها بیشتر باشد (نه یک ضریب سخت‌گیرانه‌تر مثل ۱.۵x یا ۲x) - و اگر
# داده‌ی حجم کافی/معتبر در دسترس نبود، سیگنال مسدود نمی‌شود (fail-open).
ONE_MINUTE_VOLUME_FILTER_ENABLED = True
ONE_MINUTE_VOLUME_TREND_LOOKBACK = 3   # معادل volTrendLookback در اسکریپت مرجع

# ---------------- مدیریت معامله: حد ضرر + ۴ تارگت + تریلینگ رانر ----------------
# نسخه‌ی جدید (طبق درخواست کاربر + تصویر/کد RiskRivard_System ارسالی)، جایگزین کامل
# سیستم قبلی «درصد از باقی‌مانده». تفاوت‌های کلیدی نسبت به قبل:
#   ۱) تارگت‌ها دیگر ۱R/۲R/۳R/۴R نیستند - حالا ۱R/۲R/۴R/۶R هستند (فاصله‌ی بیشتر بین
#      تارگت‌ها، مخصوصاً بعد از تارگت ۲، دقیقاً طبق تصویر ارسالی).
#   ۲) درصد بسته‌شدن هر تارگت دیگر «درصد از باقی‌مانده» نیست - مستقیماً «درصد از کل
#      حجم اولیه‌ی پوزیشن» است (ساده‌تر و دقیقاً مطابق کد پایتون ارسالی):
#        تارگت ۱ (۱R): بستن ۲۰٪ کل پوزیشن + انتقال حد ضرر به نقطه‌ی ورود (ریسک‌فری)
#        تارگت ۲ (۲R): بستن ۳۰٪ کل پوزیشن (جمعاً ۵۰٪ بسته شده) + انتقال حد ضرر به سطح تارگت ۱
#        تارگت ۳ (۴R): بستن ۱۵٪ کل پوزیشن (جمعاً ۶۵٪ بسته شده) + انتقال حد ضرر به سطح تارگت ۲
#        تارگت ۴ (۶R): بستن ۱۰٪ کل پوزیشن (جمعاً ۷۵٪ بسته شده) + فعال‌شدن تریلینگ روی
#                       ۲۵٪ باقی‌مانده («رانر»)
#   ۳) تریلینگ رانر دیگر بر اساس EMA7 نیست - بر اساس «بالاترین/پایین‌ترین قیمت رسیده
#      بعد از تارگت ۴» منهای/بعلاوه‌ی ۱.۵R (دقیقاً مثل تابع check_price در کد ارسالی).
#   ۴) نتیجه‌ی نهایی هر معامله (final_r) دیگر یک فرمول تقریبی نیست - دقیقاً از مجموع
#      «حجم بسته‌شده × R واقعی به‌دست‌آمده در هر مرحله» محاسبه می‌شود (trade["fills"]).
SWING_LOOKBACK = 10          # چند کندل قبل از کندل سیگنال، برای پیدا کردن آخرین سوینگ لو/های
SL_BUFFER_ATR_MULT = 0.75    # بافر اضافه فراتر از EMA7/سوینگ
HIST_KEEP = 80                # تعداد کندل نگه‌داشته‌شده برای رسم چارت (به‌جای فقط ۲ کندل قبلی)

# هر آیتم: شماره‌ی تارگت (۱ تا ۴)، فاصله‌ی آن بر حسب R، درصدی از کل پوزیشن که در آن
# تارگت بسته می‌شود، و اینکه بعد از رسیدن به آن، حد ضرر به کجا منتقل می‌شود
# ("entry"/"t1"/"t2"/None). تارگت ۴ به‌جای انتقال حد ضرر، تریلینگ رانر را فعال می‌کند.
TARGET_LEVELS = [
    {"n": 1, "r": 1, "close_frac": 0.20, "sl_after": "entry"},
    {"n": 2, "r": 2, "close_frac": 0.30, "sl_after": "t1"},
    {"n": 3, "r": 4, "close_frac": 0.15, "sl_after": "t2"},
    {"n": 4, "r": 6, "close_frac": 0.10, "sl_after": None, "activates_trailing": True},
]
RR_TARGETS = [lvl["r"] for lvl in TARGET_LEVELS]                 # [1, 2, 4, 6] - برای سازگاری با کدهای قدیمی که هنوز به این لیست نیاز دارند
TARGET_LABELS = {lvl["n"]: f"Target {lvl['n']} ({lvl['r']}R)" for lvl in TARGET_LEVELS}
RUNNER_FRACTION = 1 - sum(lvl["close_frac"] for lvl in TARGET_LEVELS)  # ۲۵٪ باقی‌مانده به‌عنوان رانر
TRAILING_DISTANCE_R = 1.5     # فاصله‌ی تریلینگ رانر از بالاترین/پایین‌ترین قیمت رسیده، بر حسب R (دقیقاً مثل کد ارسالی)

# برچسب انگلیسی هر نوع بسته‌شدن معامله - برای گزارش روزانه‌ی برد/باخت استفاده می‌شود.
# کلمه‌ی win/breakeven/loss در متن پیام خروج بر اساس علامت واقعی final_r محاسبه
# می‌شود (نه از این دیکشنری)، تا همیشه دقیق باشد.
CLOSE_TYPE_LABELS_EN = {
    "stop": "Stop hit before Target 1 (full loss)",
    "breakeven": "Stopped at breakeven after Target 1 (20% banked at +1R)",
    "stop_at_t1": "Stopped at Target 1 level after Target 2 (50% banked)",
    "stop_at_t2": "Stopped at Target 2 level after Target 3 (65% banked)",
    "runner_stop": "Runner closed by trailing stop after Target 4 (75% banked)",
    "opposite_signal": "Closed early - opposite signal fired",
}

# تایم‌فریم‌هایی که سیگنال کندلی برایشان بررسی می‌شود. طبق درخواست صریح کاربر: این
# کانال فقط تایم‌فریم «زیر ۴ ساعت» دارد (از ۱ ساعته تا ۱ دقیقه‌ای) - ۴ساعته و
# هفتگی که قبلاً وجود داشتند کامل حذف شدند تا کل سهمیه‌ی Twelve Data (۸۰۰
# درخواست/روز در پلن رایگان) روی همین بازه‌ی پایین متمرکز بماند.
#
# 🆕 اصلاح (طبق بازبینی برنامه‌نویس - نگرانی مصرف سهمیه‌ی API درست بود، ولی راه‌حلِ
# پیشنهادی‌اش - کم‌کردن تعداد کندلِ هر درخواست از ۱۰ به ۵ - غلط بود؛ چون هزینه‌ی هر
# درخواست در Twelve Data صرف‌نظر از تعداد کندل همیشه ۱ کردیت است، کم‌کردن تعداد کندل
# هیچ سهمیه‌ای صرفه‌جویی نمی‌کند و فقط بافر رهگیری/catch-up را کوچک‌تر (و ریسک از
# قلم‌افتادن کندل را بیشتر) می‌کند). محاسبه‌ی نسخه‌ی قبلی این فایل هم ناقص بود: فرض
# «۱۰ کندل × هر ۵ دقیقه = هیچ کندلی از قلم نمی‌افتد» فقط برای ۱ نماد درست بود، ولی
# وقتی روی ۴ نماد ضرب می‌شد (۴ × ۲۸۸ درخواست/روز فقط برای ۱ دقیقه‌ای) به‌تنهایی از
# کل سهمیه‌ی روزانه عبور می‌کرد. راه‌حل درست (برعکس پیشنهاد بازبینی‌کننده): فرکانسِ
# درخواست را کم کن، نه تعداد کندلِ هر درخواست را - و برای جبرانِ فاصله‌ی بیشتر بین
# دو چک، تعداد کندلِ درخواستی را (که رایگان است) زیاد کن تا هیچ کندلی گم نشود.
# برای همین «۱ دقیقه‌ای» الان هر ۳۰ دقیقه (نه هر ۵ دقیقه) چک می‌شود ولی هر بار
# آخرین ۴۰ کندل (نه ۱۰) می‌خواند - ۳۰ کندل برای پوشش کامل فاصله لازم است، ۴۰ حاشیه‌ی
# امنِ اضافه می‌دهد؛ دقتِ سیگنال هنوز دقیقاً روی بسته‌شدن کندل ۱ دقیقه‌ای است، فقط
# کشفش تا ۳۰ دقیقه (به‌جای ۵ دقیقه) عقب‌تر ممکن است باشد - مصالحه‌ی لازم برای ماندن
# زیر سقف سهمیه با واچ‌لیست چند-نمادی (جزئیات کامل محاسبه‌ی سهمیه در تعریف
# WATCHLIST_SYMBOLS پایین‌تر).
# 🆕 طبق درخواست صریح کاربر: تایم‌فریم‌های ۱ دقیقه و ۳۰ دقیقه کامل حذف شدند تا
# سهمیه‌ی روزانه‌ی Twelve Data آزاد شود (۱ دقیقه‌ای به‌تنهایی گران‌ترین تایم‌فریم از
# نظر تعداد درخواست بود). تایم‌فریم‌های فعال الان فقط ۵د/۱۵د/۱ساعته‌اند - دقیقاً همان
# سه تایم‌فریمی که واچ‌لیست اصلی (پایین‌تر) با فرکانس کامل چک می‌کند.
TIMEFRAMES = {
    "5m":  {"td_interval": "5min",  "bar_seconds": 5 * 60,           "label": "5M",  "catchup_limit": 10},
    "15m": {"td_interval": "15min", "bar_seconds": 15 * 60,          "label": "15M", "catchup_limit": 10},
    "1h":  {"td_interval": "1h",    "bar_seconds": 60 * 60,          "label": "1H",  "catchup_limit": 10},
    # ⚠️ تایم‌فریم ۱ دقیقه‌ای عمداً از Twelve Data نمی‌آید (حتی برای ۲-۳ نماد، رصد
    # واقعاً لحظه‌ای این تایم‌فریم به هزاران درخواست در روز نیاز دارد - چند برابر کل
    # سقف رایگان ۸۰۰/روز؛ جزئیات محاسبه در تعریف TREND_ALTS_* پایین‌تر). به‌جایش از
    # همان کندل‌هایی ساخته می‌شود که خودمان از تیک‌های زنده‌ی WebSocket (رایگان و
    # بدون هیچ هزینه‌ی API) در حافظه می‌سازیم - build_1m_candle_fetch_fn/
    # get_closed_1m_candles پایین‌تر. td_interval/catchup_limit اینجا صرفاً برای
    # یکدست‌ماندن ساختار TIMEFRAMES نگه داشته شده‌اند و توسط مسیر ۱ دقیقه‌ای
    # استفاده نمی‌شوند.
    "1m":  {"td_interval": "1min",  "bar_seconds": 60,               "label": "1M",  "catchup_limit": 30},
}


def _tf_window_id(tf_key: str, now_utc: datetime) -> Optional[str]:
    """
    شناسه‌ی «پنجره‌ی زمانی جاری» برای هر تایم‌فریم (مثلاً برای ۴ساعته: کدام بازه‌ی
    ۴ساعته‌ی امروز). این شناسه در state ذخیره می‌شود تا اگر اجرای ورک‌فلو دیر شروع
    شود (تاخیر زمان‌بندی خودِ GitHub Actions - رایج در دقیقه‌ی صفر هر ساعت)، به‌جای
    از دست رفتن کامل آن پنجره (تا نوبت بعدی، که می‌تواند تا ۴ ساعت/۱ هفته طول
    بکشد)، همچنان تا قبل از شروع پنجره‌ی بعدی این یکی پردازش شود.

    هر تایم‌فریم دقیقاً به فاصله‌ی خودش (نه بیشتر، نه کمتر) پردازش می‌شود - هیچ کندلی
    هم از قلم نمی‌افتد چون تعداد کندلِ هر درخواست (catchup_limit در TIMEFRAMES) همیشه
    برای پوشش کامل فاصله‌ی بین دو پنجره کافی انتخاب شده. «5m» و «15m» هر دو روی بازه‌ی
    ۱۵دقیقه‌ای قفل شده‌اند (نه هر دور اسکنِ ۵ دقیقه‌ای) - چون چک‌کردن هرکدام در هر دور
    اسکن، وقتی روی چند نماد ضرب شود، به‌تنهایی از کل سهمیه‌ی روزانه‌ی Twelve Data عبور
    می‌کند (جزئیات محاسبه در تعریف WATCHLIST_SYMBOLS).
    """
    if tf_key == "5m":
        bucket_min = (now_utc.minute // 15) * 15
        return now_utc.strftime("%Y-%m-%d-%H-") + f"{bucket_min:02d}"
    if tf_key == "15m":
        bucket_min = (now_utc.minute // 15) * 15
        return now_utc.strftime("%Y-%m-%d-%H-") + f"{bucket_min:02d}"
    if tf_key == "1h":
        return now_utc.strftime("%Y-%m-%d-%H")
    return None


def is_timeframe_due(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> bool:
    """
    تعیین می‌کند آیا الان زمانِ چک‌کردن این تایم‌فریم هست یا نه - بر اساس «آخرین
    پنجره‌ی پردازش‌شده» (ذخیره‌شده در state)، نه ساعت/دقیقه‌ی دیوارساعت. این یعنی
    حتی اگر یک دور اسکن چند دقیقه دیر شروع شود، تا وقتی پنجره‌ی بعدی نرسیده،
    همچنان همان پنجره پردازش می‌شود - هیچ پنجره‌ای گم نمی‌شود.
    """
    if FORCE_RUN_ALL:
        return True
    window_id = _tf_window_id(tf_key, now_utc)
    last_processed = state.get("tf_windows", {}).get(tf_key)
    return last_processed != window_id


def mark_timeframe_processed(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> None:
    window_id = _tf_window_id(tf_key, now_utc)
    if window_id is None:
        return
    state.setdefault("tf_windows", {})[tf_key] = window_id

# لیست «واچ‌لیست اصلی» - نمادهایی که همه‌ی TIMEFRAMES (۵د/۱۵د/۱ساعته) را بدون
# محدودیت دارند (کلید رایگان لازم است: https://twelvedata.com). طبق درخواست صریح
# کاربر: بیت‌کوین و اتریوم کامل از این ربات حذف شدند - این کانال فقط برای
# آلت‌کوین‌های بازار کریپتوکارنسی است. کل سهمیه‌ی Twelve Data + کل ظرفیت اتصال
# زنده‌ی WebSocket که قبلاً صرف BTC/ETH می‌شد، حالا صرف همین آلت‌کوین‌ها می‌شود.
#
# 🆕 طبق تصمیم صریح کاربر (بعد از بررسی مصالحه‌ی سهمیه/تعداد نماد): واچ‌لیست اصلی
# (تایم‌فریم کامل ۵د+۱۵د+۱ساعته، + استریم زنده‌ی WebSocket) روی ۳ آلت‌کوین با بیشترین
# نقدشوندگی نگه داشته شد (SOL, XRP, ADA) - نه بیشتر - تا کاملاً زیر سقف رایگان Twelve
# Data (۸۰۰ درخواست/روز) با حاشیه‌ی امن باقی بماند؛ ۴ نماد (۸۶۴ درخواست/روز فقط
# برای همین گروه) به‌تنهایی از سقف عبور می‌کرد، قبل از احتساب ۳۰ ارز برتر/ارزهای
# پرتحرک/رصد زنده. باقی‌ی لیست تا ۱۵-۳۰ آلت‌کوین برتر بازار (طبق درخواست کاربر) از
# طریق همان مکانیزم TOP30 (پایین‌تر - تایم‌فریم ۱ساعته، ۳ بار در روز، دقیقاً همان
# منطق سیگنال/فرمت پیام) پوشش داده می‌شود - نه با تایم‌فریم‌های زیر ۱ساعته.
#
# محاسبه‌ی سهمیه‌ی روزانه با ۳ نماد (هر تایم‌فریم × فرکانس چک × ۳ نماد):
#   5m  هر ۱۵ دقیقه (۹۶ بار/روز) × ۳ = ۲۸۸
#   15m هر ۱۵ دقیقه (۹۶ بار/روز) × ۳ = ۲۸۸
#   1h  هر ۱ ساعت   (۲۴ بار/روز) × ۳ = ۷۲
#   جمع واچ‌لیست اصلی = ۶۴۸ درخواست/روز
# باقی‌مانده برای ۳۰ ارز برتر (۱ساعته، ۳بار/روز ≈ ۹۰/روز) + ارزهای پرتحرک (پایین‌تر
# ≈ ۹۰/روز) + رصد زنده‌ی REST معاملات باز غیر-واچ‌لیست (≈ ۵۰/روز،
# MAX_LIVE_CHECKS_PER_RUN) ≈ ۲۳۰ درخواست/روز - جمع کل ≈ ۸۷۸... در واقع همیشه کمتر
# از این سقفِ نظریِ بدترین‌حالت (چون نه همیشه معامله‌ی باز برای رصد هست، نه همیشه هر
# تایم‌فریمی سیگنال تازه دارد)؛ اگر عملاً هم به سقف نزدیک شد، اول TOP30_SCAN_HOURS/
# TREND_ALTS_*_SCAN_HOURS را کاهش دهید، نه واچ‌لیست اصلی را زیاد.
#
# برای اضافه/حذف نماد همین دیکشنری (و WS_SYMBOL_MAP پایین‌تر که خودکار از همین
# دیکشنری ساخته می‌شود) را ویرایش کنید - فقط توجه داشته باشید هر نماد اضافه، طبق
# محاسبه‌ی بالا، ۲۱۶ درخواست/روز به مصرف کل اضافه می‌کند.
WATCHLIST_SYMBOLS = {
    "SOL/USD": "SOL",
    "XRP/USD": "XRP",
    "ADA/USD": "ADA",
}

# ================== استریم زنده‌ی WebSocket برای واچ‌لیست آلت‌کوین (دقت زیر ۱۰ ثانیه، بدون هزینه‌ی API) ==================
# قبلاً این استریم فقط BTC/ETH را پوشش می‌داد؛ طبق درخواست کاربر («نهایت دقت و
# سرعت» برای آلت‌کوین‌ها) حالا دقیقاً همان ۲ آلت‌کوین واچ‌لیست اصلی را پوشش می‌دهد -
# یعنی معاملات باز این ۲ نماد هر ۵ ثانیه با آخرین تیک قیمت زنده (نه فقط هر چند
# دقیقه با کندل REST) چک می‌شوند؛ رصد تارگت/استاپ آن‌ها عملاً لحظه‌ای است.
# نمادهای «۳۰ ارز برتر» و «ارزهای پرتحرک» دیگر بدون هیچ رصدی بین دو اسکنِ کندل
# نمی‌مانند - طبق بازبینی برنامه‌نویس، تابع check_rest_live_trades (پایین‌تر) با
# اندپوینت سبک REST /price این شکاف را پر می‌کند؛ فقط چون تعدادشان پویا و بزرگ است،
# مشترک‌کردنشان در همان اتصال WebSocket واحد به‌صرفه نیست (پیچیدگی/محدودیت تعداد
# subscription زیاد، بدون سود متناسب نسبت به /price دوره‌ای).
WS_URL = "wss://ws-feed.exchange.coinbase.com"
WS_SYMBOL_MAP = {sym: sym.replace("/", "-") for sym in WATCHLIST_SYMBOLS}
WS_SYMBOL_MAP_REVERSE = {v: k for k, v in WS_SYMBOL_MAP.items()}


def parse_ws_ticker_message(raw_message: str) -> Optional[Dict[str, Any]]:
    """یک پیام ticker از Coinbase را پارس می‌کند و {"symbol": "BTC/USD", "price": 65000.1,
    "size": 0.42} برمی‌گرداند، یا None اگر پیام مرتبط نبود. "size" حجم همان معامله‌ی
    واحدی است که این تیک را تولید کرده (فیلد last_size کوین‌بیس) - برای ساخت حجم
    کندل ۱ دقیقه‌ای (feed_1m_candle_tick) روی هم جمع زده می‌شود. جدا از اتصال شبکه
    نوشته شده تا بدون نیاز به سرور واقعی قابل تست باشد."""
    try:
        data = json.loads(raw_message)
    except Exception:
        return None
    if data.get("type") != "ticker":
        return None
    product_id = data.get("product_id")
    price_str = data.get("price")
    if not product_id or not price_str or product_id not in WS_SYMBOL_MAP_REVERSE:
        return None
    try:
        size_str = data.get("last_size")
        size = float(size_str) if size_str is not None else 0.0
        return {"symbol": WS_SYMBOL_MAP_REVERSE[product_id], "price": float(price_str), "size": size}
    except (TypeError, ValueError):
        return None


def start_price_stream(latest_prices: Dict[str, float], price_lock: threading.Lock, stop_event: threading.Event) -> None:
    """در یک ترد جداگانه اجرا می‌شود؛ تا وقتی stop_event ست نشده، مدام سعی می‌کند وصل
    بماند و با هر تیک قیمت، latest_prices را آپدیت می‌کند (بدون هیچ هزینه‌ی درخواست API).
    ⚠️ اتصال واقعی این بخش قابل تست از محیط توسعه نیست (دسترسی شبکه به Coinbase وجود
    ندارد) - منطق پردازش پیام (parse_ws_ticker_message) کامل تست‌شده، ولی خودِ اتصال
    باید بعد از دیپلوی از لاگ Actions تایید شود. اگر وصل نشود، رهگیری REST دوره‌ای
    (fetch_live_price در run_scan_cycle) همچنان به‌عنوان پشتیبان کار می‌کند.

    🆕 علاوه بر واچ‌لیست اصلی (WS_SYMBOL_MAP)، از ابتدای هر اجرا به ارزهای همسو با
    روندِ همان روز (TREND_ALTS_WS_SYMBOL_MAP - قبل از شروع این ترد در main() پر
    می‌شود) هم subscribe می‌شود؛ چون انتخاب این لیست فقط یک‌بار در روز است و هر اجرا
    خودش حداکثر ~۵ ساعت و ۲۰ دقیقه طول می‌کشد، نیازی به subscribe/unsubscribe پویای
    وسط اجرا نیست - اگر لیست وسط یک اجرا (نادر - فقط دور نیمه‌شب) عوض شود، اجرای
    بعدی (حداکثر تا ۵ ساعت و ۲۰ دقیقه‌ی دیگر) آن را می‌گیرد."""
    try:
        import websocket
    except ImportError:
        logger.error("⚠️ کتابخانه‌ی websocket-client نصب نیست؛ استریم زنده غیرفعال می‌ماند (فقط REST پشتیبان کار می‌کند)")
        return

    product_ids = list(WS_SYMBOL_MAP.values()) + list(TREND_ALTS_WS_SYMBOL_MAP.values())

    def on_open(ws):
        logger.info(f"🔌 WebSocket وصل شد - در حال subscribe به {product_ids}")
        ws.send(json.dumps({"type": "subscribe", "product_ids": product_ids, "channels": ["ticker"]}))

    def on_message(ws, message):
        parsed = parse_ws_ticker_message(message)
        if parsed:
            with price_lock:
                latest_prices[parsed["symbol"]] = parsed["price"]
            if parsed["symbol"] in TREND_ALTS_WS_SYMBOL_MAP:
                feed_1m_candle_tick(parsed["symbol"], parsed["price"], time.time(), parsed.get("size", 0.0))

    def on_error(ws, error):
        logger.warning(f"⚠️ خطای WebSocket: {error}")

    def on_close(ws, code, msg):
        logger.warning(f"⚠️ اتصال WebSocket قطع شد (code={code}, msg={msg})")

    backoff = 5
    while not stop_event.is_set():
        try:
            ws_app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message,
                                             on_error=on_error, on_close=on_close)
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
            backoff = 5  # اتصال موفق بود، بعد از قطع طبیعی دوباره سریع امتحان کن
        except Exception as e:
            logger.error(f"❌ اتصال WebSocket ناموفق بود: {e}")
        if stop_event.is_set():
            break
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)  # افزایش نمایی تا سقف ۶۰ ثانیه


def check_ws_open_trades(bot: "TradeiscoolBot", state: Dict[str, Any],
                          latest_prices: Dict[str, float], price_lock: threading.Lock) -> None:
    """معاملات باز واچ‌لیست آلت‌کوین را با آخرین قیمتِ رسیده از WebSocket چک می‌کند - این
    تابع هر چند ثانیه یک‌بار در حلقه‌ی اصلی صدا زده می‌شود، بدون هیچ هزینه‌ی API. دقیقاً
    همین state (نه کپی از دیسک) به‌روزرسانی می‌شود تا با بقیه‌ی حلقه هماهنگ بماند؛
    ذخیره‌ی نهایی روی دیسک را git_commit_and_push/save_state دوره‌ای انجام می‌دهد."""
    with price_lock:
        prices_snapshot = dict(latest_prices)
    if not prices_snapshot:
        return

    candle_states = state.setdefault("candle_signals", {})
    trade_history = state.setdefault("trade_history", [])
    target_events = state.setdefault("target_events", [])

    # 🆕 ارزهای همسو با روندِ همین روز هم اضافه می‌شوند - همان استریم واحد WebSocket
    # حالا معاملات باز این‌ها را هم (روی هر ۴ تایم‌فریم، از جمله ۱د) لحظه‌ای رصد می‌کند.
    display_map = dict(WATCHLIST_SYMBOLS)
    symbol_map = dict(WS_SYMBOL_MAP)
    for mover in state.get("trend_alts", {}).get("candidates", []):
        td_symbol = f"{mover['symbol']}/USD"
        if td_symbol in TREND_ALTS_WS_SYMBOL_MAP:
            display_map[td_symbol] = mover["symbol"]
            symbol_map[td_symbol] = TREND_ALTS_WS_SYMBOL_MAP[td_symbol]

    for symbol in symbol_map:
        live_price = prices_snapshot.get(symbol)
        if not live_price:
            continue
        display_name = display_map.get(symbol, symbol)
        for tf_key, tf_cfg in TIMEFRAMES.items():
            state_key = f"{symbol}|{tf_key}"
            sym_state = candle_states.get(state_key)
            if not sym_state:
                continue
            trade = sym_state.get("open_trade")
            if not trade or trade.get("closed"):
                continue
            events = check_open_trade_live(trade, live_price, symbol_for_log=symbol)
            for ev in events:
                send_trade_exit(bot, symbol, display_name, tf_key, tf_cfg["label"], trade, ev,
                                 sym_state["hist"], trade_history=trade_history, live_price=live_price,
                                 target_events=target_events, global_state=state)


def check_rest_live_trades(bot: "TradeiscoolBot", state: Dict[str, Any]) -> None:
    """طبق بازبینی برنامه‌نویس: معاملات باز واچ‌لیست اصلی از طریق WebSocket (رایگان و
    لحظه‌ای) رصد می‌شوند، ولی معاملات باز ۳۰ ارز برتر و ارزهای پرتحرک قبلاً هیچ رصد
    زنده‌ای نداشتند و فقط در دور اسکن کندلی بعدی (که ممکن است تا ۱ ساعت طول بکشد)
    کشف می‌شدند. این تابع همان شکاف را با اندپوینت سبک /price Twelve Data (که قبلاً
    فقط برای طلا نوشته شده بود و بعد از حذف طلا بلااستفاده مانده بود - fetch_live_price)
    پر می‌کند: هر ۳۰ دقیقه (LIVE_CHECK_INTERVAL_SECONDS) حداکثر MAX_LIVE_CHECKS_PER_RUN
    معامله‌ی باز (به‌صورت چرخشی/rotating تا هیچ‌کدام همیشه گرسنه نمانند) با یک درخواست
    سبک قیمت لحظه‌ای چک می‌شوند - هزینه‌ی ناچیز در سهمیه‌ی روزانه (حداکثر ۴۸ درخواست/روز
    در بدترین حالت) در ازای کشف بسیار سریع‌تر تارگت/استاپ."""
    candle_states = state.setdefault("candle_signals", {})
    trade_history = state.setdefault("trade_history", [])
    target_events = state.setdefault("target_events", [])

    open_keys = []
    for state_key, sym_state in candle_states.items():
        try:
            symbol, tf_key = state_key.split("|", 1)
        except ValueError:
            continue
        if symbol in WS_SYMBOL_MAP or symbol in TREND_ALTS_WS_SYMBOL_MAP:
            continue  # این‌ها از قبل رایگان و لحظه‌ای از طریق WebSocket رصد می‌شوند
        trade = (sym_state or {}).get("open_trade")
        if trade and not trade.get("closed"):
            open_keys.append(state_key)
    if not open_keys:
        return

    open_keys.sort()
    cursor = state.get("live_check_cursor", 0) % len(open_keys)
    ordered = open_keys[cursor:] + open_keys[:cursor]
    batch = ordered[:MAX_LIVE_CHECKS_PER_RUN]
    state["live_check_cursor"] = (cursor + len(batch)) % len(open_keys)

    for state_key in batch:
        symbol, tf_key = state_key.split("|", 1)
        sym_state = candle_states.get(state_key)
        if not sym_state:
            continue
        trade = sym_state.get("open_trade")
        if not trade or trade.get("closed"):
            continue
        live_price = fetch_live_price(symbol)
        if not live_price:
            continue
        tf_cfg = TIMEFRAMES.get(tf_key)
        if not tf_cfg:
            continue
        display_name = symbol.split("/")[0]
        events = check_open_trade_live(trade, live_price, symbol_for_log=symbol)
        for ev in events:
            send_trade_exit(bot, symbol, display_name, tf_key, tf_cfg["label"], trade, ev,
                             sym_state["hist"], trade_history=trade_history, live_price=live_price,
                             target_events=target_events, global_state=state)

def check_trend_alts_1m_signals(bot: "TradeiscoolBot", state: Dict[str, Any]) -> None:
    """تایم‌فریم ۱ دقیقه‌ای ارزهای همسو با روند را از کندل‌های ساخته‌شده در حافظه
    (get_closed_1m_candles - از تیک‌های WebSocket، نه Twelve Data) چک می‌کند. این
    تابع هر چند ثانیه در حلقه‌ی اصلی صدا زده می‌شود (نه هر ۵ دقیقه مثل بقیه‌ی
    تایم‌فریم‌ها) - چون هیچ هزینه‌ی API ندارد، سیگنال ۱ دقیقه‌ای عملاً چند ثانیه بعد
    از بسته‌شدنِ واقعیِ کندل کشف و در کانال فرستاده می‌شود."""
    candle_states = state.setdefault("candle_signals", {})
    trend_alts = get_trend_alts_to_process(state, candle_states)
    if not trend_alts:
        return
    trade_history = state.setdefault("trade_history", [])
    target_events = state.setdefault("target_events", [])
    tf_cfg = TIMEFRAMES["1m"]

    for mover in trend_alts:
        cmc_symbol = mover["symbol"]
        td_symbol = f"{cmc_symbol}/USD"
        if td_symbol not in TREND_ALTS_WS_SYMBOL_MAP:
            continue  # این نماد به WebSocket این اجرا وصل نشده (نادر) - اجرای بعدی می‌گیرد
        state_key = f"{td_symbol}|1m"
        sym_state = candle_states.get(state_key)
        fetch_fn = lambda sym, lim: get_closed_1m_candles(sym, lim)
        try:
            new_state, sent, err = process_and_send_symbol_tf(
                bot, fetch_fn, td_symbol, cmc_symbol, "1m", tf_cfg, sym_state,
                trade_history=trade_history, source_label=f"ارز همسو با روند ({mover['reason']})",
                target_events=target_events, global_state=state)
            if new_state is not None:
                candle_states[state_key] = new_state
        except Exception as e:
            logger.warning(f"⚠️ خطا در پردازش تایم‌فریم ۱ دقیقه‌ای (WebSocket) برای {td_symbol}: {e}")


# ۳۰ ارز برتر بازار (بر اساس مارکت‌کپ، به‌صورت زنده از CoinMarketCap؛ BTC/ETH طبق
# درخواست کاربر همیشه از این لیست حذف می‌شوند - کانال کاملاً اختصاصی آلت‌کوین است).
# طبق درخواست کاربر همه‌ی تایم‌فریم‌های این ربات باید زیر ۴ ساعت باشند، پس این گروه
# از ۴ساعته به ۱ساعته منتقل شد (بالاترین تایم‌فریمِ مجاز باقی‌مانده). دلیل محدودیت
# فرکانس: با سهمیه‌ی رایگان Twelve Data (۸۰۰ درخواست/روز) امکان چک‌کردن ۳۰ نماد با
# فرکانس معنادار وجود ندارد (هر دور کامل این گروه به‌تنهایی ۳۰ درخواست است) - به
# همین دلیل این گروه با فرکانس کم (۳ بار در روز) چک می‌شود تا مجموع سهمیه زیر
# ۸۰۰ درخواست در روز بماند و سهمیه‌ی بیشتری برای واچ‌لیست اصلی/ارزهای پرتحرک
# (که واقعاً به سرعت/دقت تایم‌فریم پایین نیاز دارند) آزاد بماند.
TOP30_ENABLED = True
TOP30_COUNT = 30
TOP30_TIMEFRAME = "1h"
TOP30_SCAN_HOURS = [0, 8, 16]  # ۳ بار در روز

# ---------------- ارزهای همسو با روند بازار (نسبت به BTC، انتخاب روزانه) ----------------
# طبق درخواست کاربر: به‌جای «۳ برنده + ۳ بازنده» ثابت (نسخه‌ی قبلی «ارزهای پرتحرک»)،
# اول جهت کلی بازار (صعودی/نزولی/خنثی) از روی درصد تغییر ۲۴ساعته‌ی خودِ BTC تشخیص
# داده می‌شود (get_market_regime) - بدون هیچ هزینه‌ی API اضافه، چون از همان لیست
# CoinMarketCap که برای انتخاب کاندیدها هم لازم است استفاده می‌کند. سپس فقط یک طرف
# انتخاب می‌شود: اگر بازار صعودی → بیشترین صعود نسبت به BTC، اگر نزولی → بیشترین
# نزول نسبت به BTC. لیست فقط یک‌بار در روز (TREND_ALTS_REFRESH_HOUR، پیش‌فرض ۰۰:۰۰
# UTC) بروز می‌شود - نه هر ۳ ساعت مثل نسخه‌ی قبلی - چون این‌بار هدف «همسویی با روند
# روزانه‌ی بازار» است، نه واکنش لحظه‌ای به نوسان کوتاه‌مدت.
#
# 🆕 محاسبه‌ی سهمیه (طبق تصمیم صریح کاربر بعد از بررسی مصالحه‌ی سرعت/تعداد): چون
# تایم‌فریم ۱ دقیقه‌ای دیگر اصلاً از Twelve Data نمی‌آید (پایین‌تر توضیح داده شده -
# از WebSocket رایگان می‌آید)، این گروه فقط برای ۵د/۱۵د/۱ساعته سهمیه مصرف می‌کند.
# با ۳ ارز، دقیقاً هم‌اندازه‌ی همان ~۶۶ درخواست/روزی که با حذف «ارزهای پرتحرک»ِ قبلی
# (۶ ارز) آزاد شد نگه داشته شده - فقط با فرکانس بهتر چون تعداد ارز نصف شده:
#   1h  هر ۲ ساعت (۱۲بار/روز) × ۳ = ۳۶
#   15m هر ۴ ساعت (۶بار/روز)  × ۳ = ۱۸
#   5m  هر ۶ ساعت (۴بار/روز)  × ۳ = ۱۲
#   جمع = ۶۶ درخواست/روز (دقیقاً سطح امنِ قبلی، سهمیه‌ی کل سیستم تغییر نمی‌کند)
TREND_ALTS_ENABLED = True
TREND_ALTS_COUNT = 3
TREND_ALTS_REFRESH_HOUR = 0     # هر روز ساعت ۰۰:۰۰ UTC یک‌بار لیست جدید انتخاب می‌شود
# 🆕 برای اینکه سیستم «قطع خودکار بعد از استاپ‌های پیاپی» (پایین‌تر) بتواند فوراً یک
# ارز جایگزین معرفی کند، هر روز به‌جای فقط ۳ ارز، یک استخر رتبه‌بندی‌شده از ۱۰ کاندید
# ذخیره می‌شود - هزینه‌ی API اضافه ندارد (همان یک درخواست CoinMarketCap، فقط برش
# بزرگ‌تر). در حالت عادی فقط ۳ تای اول (بالاترین امتیاز) واقعاً سیگنال می‌دهند.
TREND_ALTS_CANDIDATE_POOL_SIZE = 10
# اگر درصد تغییر ۲۴ساعته‌ی BTC داخل این بازه‌ی خنثی باشد (نه واقعاً صعودی نه نزولی)،
# آن روز هیچ ارزی انتخاب نمی‌شود (بهتر از انتخاب تصادفی/بی‌معنا در بازار بلاتکلیف).
MARKET_REGIME_NEUTRAL_BAND_PCT = 0.5
TREND_ALTS_TIMEFRAMES = ["1h", "15m", "5m"]   # از Twelve Data REST - تایم‌فریم ۱د جدا و از WebSocket پردازش می‌شود
TREND_ALTS_1H_SCAN_HOURS = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]   # هر ۲ ساعت
TREND_ALTS_15M_SCAN_HOURS = [0, 4, 8, 12, 16, 20]                        # هر ۴ ساعت
TREND_ALTS_5M_SCAN_HOURS = [0, 6, 12, 18]                                # هر ۶ ساعت

# نگاشت پویای «نماد → Coinbase product_id» برای ارزهای همسو با روندِ همین روز - در
# ابتدای main() (قبل از شروع ترد WebSocket) یک‌بار پر می‌شود تا استریم زنده هم
# معاملات باز این ارزها را لحظه‌ای رصد کند، هم کندل ۱ دقیقه‌ای‌شان را در حافظه بسازد.
TREND_ALTS_WS_SYMBOL_MAP: Dict[str, str] = {}

# ================== قطع خودکار سیگنال بعد از استاپ‌های پیاپی (روزانه، هر نماد) ==================
# طبق درخواست صریح کاربر (بعد از یک روز فاجعه‌بار که یک ارز مرتب سیگنال می‌داد و پشت
# سرهم استاپ می‌خورد): اگر یک نماد در همان روز (UTC) بیش از این تعداد بار واقعاً استاپ
# لاس کامل بخورد (نوع رویداد "stop" - یعنی ضرر خالص قبل از رسیدن به هیچ تارگتی؛
# breakeven/stop_at_t1/stop_at_t2 که همه سودِ قفل‌شده هستند حساب نمی‌شوند)، تا پایان
# همان روز دیگر هیچ سیگنال ورودِ جدیدی برای آن نماد صادر نمی‌شود - معاملات باز موجودش
# اما همچنان تا بسته‌شدنِ طبیعی رصد و گزارش می‌شوند (چیزی رها نمی‌شود). برای گروه
# «ارزهای همسو با روند» که یک استخر کاندید دارد، ارز قفل‌شده بلافاصله با نفر بعدیِ
# استخر (get_active_trend_alts) جایگزین می‌شود - دقیقاً طبق درخواست کاربر.
MAX_DAILY_STOPS_PER_SYMBOL = 3


def _daily_stop_bucket(state: Dict[str, Any], now_utc: datetime) -> Dict[str, Any]:
    today = now_utc.strftime("%Y-%m-%d")
    bucket = state.setdefault("daily_stop_counts", {})
    if bucket.get("date") != today:
        bucket.clear()
        bucket["date"] = today
        bucket["counts"] = {}
    return bucket


def record_stop_loss(display_name: str, state: Dict[str, Any]) -> None:
    now_utc = datetime.now(timezone.utc)
    bucket = _daily_stop_bucket(state, now_utc)
    counts = bucket.setdefault("counts", {})
    counts[display_name] = counts.get(display_name, 0) + 1
    if counts[display_name] == MAX_DAILY_STOPS_PER_SYMBOL + 1:
        logger.warning(f"⛔ {display_name} امروز {counts[display_name]} بار استاپ لاس واقعی خورد - "
                        f"تا پایان امروز (UTC) دیگر سیگنال ورود جدیدی برایش صادر نمی‌شود")


def is_symbol_blocked_today(display_name: str, state: Dict[str, Any]) -> bool:
    now_utc = datetime.now(timezone.utc)
    bucket = _daily_stop_bucket(state, now_utc)
    return bucket.get("counts", {}).get(display_name, 0) > MAX_DAILY_STOPS_PER_SYMBOL


def get_active_trend_alts(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """اولین TREND_ALTS_COUNT کاندید (از استخر رتبه‌بندی‌شده‌ی امروز) که هنوز قفل‌شده‌ی
    امروز نیستند - یعنی اگر کاندید رتبه‌ی اول امروز به‌خاطر استاپ‌های پیاپی قفل شود،
    خودکار با نفر بعدیِ استخر جایگزین می‌شود."""
    candidates = state.get("trend_alts", {}).get("candidates", [])
    active = []
    for c in candidates:
        if not is_symbol_blocked_today(c["symbol"], state):
            active.append(c)
        if len(active) >= TREND_ALTS_COUNT:
            break
    return active


def get_trend_alts_to_process(state: Dict[str, Any], candle_states: Dict[str, Any]) -> List[Dict[str, Any]]:
    """لیستی که واقعاً باید پردازش شود = کاندیدهای فعال امروز (حداکثر TREND_ALTS_COUNT
    تا، برای کنترل سهمیه) + هر کاندید قفل‌شده‌ای که همین الان یک معامله‌ی باز دارد (تا
    آن معامله رها نشود و حتماً تا بسته‌شدنِ طبیعی‌اش رصد/گزارش شود)."""
    active = get_active_trend_alts(state)
    active_syms = {c["symbol"] for c in active}
    all_tfs = TREND_ALTS_TIMEFRAMES + ["1m"]
    extra = []
    for c in state.get("trend_alts", {}).get("candidates", []):
        if c["symbol"] in active_syms:
            continue
        td_symbol = f"{c['symbol']}/USD"
        has_open = any(
            ((candle_states.get(f"{td_symbol}|{tf}") or {}).get("open_trade") or {}).get("closed") is False
            for tf in all_tfs
        )
        if has_open:
            extra.append(c)
    return active + extra

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


def _trend_alts_window_id(tf_key: str, now_utc: datetime) -> str:
    if tf_key == "1h":
        bucket_hour = max(h for h in TREND_ALTS_1H_SCAN_HOURS if h <= now_utc.hour)
        return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"
    if tf_key == "15m":
        bucket_hour = max(h for h in TREND_ALTS_15M_SCAN_HOURS if h <= now_utc.hour)
        return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"
    bucket_hour = max(h for h in TREND_ALTS_5M_SCAN_HOURS if h <= now_utc.hour)
    return now_utc.strftime("%Y-%m-%d-") + f"{bucket_hour:02d}"


def is_trend_alts_tf_due(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> bool:
    if FORCE_RUN_ALL:
        return True
    window_id = _trend_alts_window_id(tf_key, now_utc)
    last_processed = state.get("tf_windows", {}).get(f"trendalts_{tf_key}")
    return last_processed != window_id


def mark_trend_alts_tf_processed(tf_key: str, now_utc: datetime, state: Dict[str, Any]) -> None:
    window_id = _trend_alts_window_id(tf_key, now_utc)
    state.setdefault("tf_windows", {})[f"trendalts_{tf_key}"] = window_id


def _trend_alts_refresh_window_id(now_utc: datetime) -> str:
    """یک‌بار در روز - از ساعت TREND_ALTS_REFRESH_HOUR به بعد، تا قبل از همان ساعت
    فردا، همین یک پنجره فعال است (دقیقاً مثل نتیجه‌ی گزارش روزانه - is_results_report_due)."""
    bucket_date = now_utc.strftime("%Y-%m-%d")
    if now_utc.hour < TREND_ALTS_REFRESH_HOUR:
        # قبل از ساعت بروزرسانی امروز هنوز - یعنی پنجره‌ی «دیروز» ادامه دارد
        prev_day = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        prev_day = datetime.fromtimestamp(prev_day.timestamp() - 86400, tz=timezone.utc)
        bucket_date = prev_day.strftime("%Y-%m-%d")
    return bucket_date


def is_trend_alts_refresh_due(now_utc: datetime, state: Dict[str, Any]) -> bool:
    if FORCE_RUN_ALL:
        return True
    last_processed = state.get("tf_windows", {}).get("trendalts_refresh")
    return last_processed != _trend_alts_refresh_window_id(now_utc)


def mark_trend_alts_refresh_processed(now_utc: datetime, state: Dict[str, Any]) -> None:
    state.setdefault("tf_windows", {})["trendalts_refresh"] = _trend_alts_refresh_window_id(now_utc)


def get_market_regime(top_coins: List[Dict[str, Any]]) -> Tuple[str, float]:
    """جهت کلی بازار را از روی درصد تغییر ۲۴ساعته‌ی خودِ BTC تشخیص می‌دهد - بدون هیچ
    درخواست API اضافه (از همان لیست CoinMarketCap که برای انتخاب کاندیدها هم لازم
    است). برمی‌گرداند: ("bull"/"bear"/"neutral", درصد تغییر BTC)."""
    btc_data = next((c for c in top_coins if c.get('symbol') == 'BTC'), None)
    if not btc_data:
        return "neutral", 0.0
    btc_change = safe_get(btc_data, "quote", "USD", "percent_change_24h") or 0.0
    if btc_change > MARKET_REGIME_NEUTRAL_BAND_PCT:
        return "bull", btc_change
    if btc_change < -MARKET_REGIME_NEUTRAL_BAND_PCT:
        return "bear", btc_change
    return "neutral", btc_change


def build_trend_alts_ws_map(state: Dict[str, Any]) -> Dict[str, str]:
    """نگاشت «TICKER/USD → TICKER-USD» برای ارزهای همسو با روندِ *فعالِ* همین روز - برای
    subscribe کردن استریم WebSocket + ساخت کندل ۱ دقیقه‌ای. فقط لیست فعال (get_active_trend_alts،
    نه کل استخر ۱۰تایی) subscribe می‌شود تا هزینه‌ی بی‌مورد به استریم اضافه نشود."""
    result = {}
    for mover in get_active_trend_alts(state):
        td_symbol = f"{mover['symbol']}/USD"
        result[td_symbol] = td_symbol.replace("/", "-")
    return result


# ================== ساخت کندل ۱ دقیقه‌ای در حافظه از تیک‌های زنده‌ی WebSocket ==================
# چرا لازم است: طبق محاسبه‌ی بالا، رصد REST تایم‌فریم ۱ دقیقه‌ای برای چند نماد از کل
# سقف رایگان Twelve Data (۸۰۰/روز) عبور می‌کند. چون همین الان یک اتصال WebSocket
# زنده و رایگان به Coinbase برای ارزهای همسو با روند برقرار است (برای رصد معاملات
# باز)، همان تیک‌های قیمت برای ساختن کندل ۱ دقیقه‌ای هم استفاده می‌شوند - هزینه‌ی
# اضافه صفر، و چون کندل بسته‌شده بلافاصله (نه با تاخیر چند دقیقه/ساعته‌ی REST) در
# دسترس است، سیگنال ۱ دقیقه‌ای عملاً در همان ثانیه‌ی بسته‌شدن کندل کشف می‌شود.
_candle_builders: Dict[str, Dict[str, Any]] = {}
_candle_builder_lock = threading.Lock()


def feed_1m_candle_tick(symbol: str, price: float, ts: float, size: float = 0.0) -> None:
    bucket_start = int(ts // 60) * 60
    with _candle_builder_lock:
        b = _candle_builders.setdefault(symbol, {"cur": None, "closed": []})
        cur = b["cur"]
        if cur is None or cur["bucket"] != bucket_start:
            if cur is not None:
                b["closed"].append({
                    "open_time": int(cur["bucket"] * 1000),
                    "o": cur["o"], "h": cur["h"], "l": cur["l"], "c": cur["c"], "v": cur["v"],
                })
                b["closed"] = b["closed"][-60:]   # نگه‌داشتن ~۱ ساعت آخر کافی است (catchup_limit=30)
            b["cur"] = {"bucket": bucket_start, "o": price, "h": price, "l": price, "c": price, "v": size}
        else:
            if price > cur["h"]:
                cur["h"] = price
            if price < cur["l"]:
                cur["l"] = price
            cur["c"] = price
            cur["v"] = cur.get("v", 0.0) + size


def get_closed_1m_candles(symbol: str, limit: int) -> List[Dict[str, Any]]:
    """امضای این تابع عمداً دقیقاً مثل fetch_closed_klines_twelvedata(symbol, limit)
    است - یعنی می‌تواند مستقیم به‌عنوان fetch_fn به process_and_send_symbol_tf داده
    شود، بدون هیچ تغییری در آن تابع مشترک."""
    with _candle_builder_lock:
        b = _candle_builders.get(symbol)
        if not b:
            return []
        return list(b["closed"])[-limit:]


# گزارش روزانه‌ی خودکار عملکرد سیگنال‌ها - چون bot.py الان به‌صورت پروسه‌ی پیوسته
# اجرا می‌شود (نه یک‌بار و خروج)، این تابع هر دور اسکن (هر ۵ دقیقه) چک می‌شود که آیا
# «امروز» هنوز گزارش نرفته و ساعت از RESULTS_REPORT_HOUR گذشته - اگر بله، دقیقاً
# همان یک‌بار می‌فرستد (طبق last_date در state.json) و بلافاصله علامت را پوش می‌کند.
RESULTS_REPORT_HOUR = 8  # ساعت ۸ UTC (تهران ~۱۱:۳۰ صبح) - پایان روز معاملاتی


def is_results_report_due(now_utc: datetime, state: Dict[str, Any]) -> bool:
    if FORCE_RUN_ALL:
        return True
    last_date = state.get("tf_windows", {}).get("results_report")
    today = now_utc.strftime("%Y-%m-%d")
    return now_utc.hour >= RESULTS_REPORT_HOUR and last_date != today


def mark_results_report_processed(now_utc: datetime, state: Dict[str, Any]) -> None:
    state.setdefault("tf_windows", {})["results_report"] = now_utc.strftime("%Y-%m-%d")


def _refresh_tf_windows_from_disk(state: Dict[str, Any]) -> None:
    """پرچم‌های «این پنجره/این روز قبلاً پردازش/ارسال شده» (همه‌ی کلیدهای tf_windows -
    results_report, flow_perf, top30, trendalts_*) را از فایل تازه‌ی روی دیسک در state
    موجود merge می‌کند - فقط همین زیردیکشنری را، نه کل state، چون همین دور اسکن ممکن
    است خودش تغییرات پردازش‌نشده/پوش‌نشده‌ی دیگری (مثل candle_signals تازه) داشته
    باشد که نباید با نسخه‌ی قدیمی‌تر دیسک بازنویسی شوند. (قبلاً این تابع فقط مخصوص
    results_report بود؛ به همه‌ی کلیدهای tf_windows تعمیم داده شد - دلیل در پایین.)

    🔴 رفعِ باگِ واقعیِ «گزارش روزانه بیش از یک‌بار ارسال می‌شود» (ریشه‌ی باقی‌مانده،
    بعد از رفع نسخه‌ی قبلی): نسخه‌ی قبلی این تابع فقط جلوی این را می‌گرفت که یک
    مقدار قدیمی‌تر از دیسک، مقدار تازه‌تر در حافظه را در **ابتدای** دور اسکن بعدی
    عقب بیندازد. اما یک مسیر دوم و مستقل هنوز باز بود: در حلقه‌ی اصلی (main)، وقتی
    run_scan_cycle با خطا مواجه می‌شد، کل state (شامل همین پرچم‌ها) به نسخه‌ی backup
    *قبل از شروع همان دور* برمی‌گشت - even اگر داخل همان دور، قبل از آن خطا، پرچم
    «گزارش امروز فرستاده شد» با موفقیت mark و save و push هم شده باشد! نسخه‌ی
    reverted‌شده بلافاصله با save_state روی دیسک هم می‌نشست و دیر یا زود (کامیت
    دوره‌ای بعدی) با «-X theirs» به‌جای نسخه‌ی درستِ ریموت پوش می‌شد - یعنی پرچم از
    ریموت هم پاک می‌شد و دور بعدی دوباره due می‌دید و گزارش را دوباره می‌فرستاد.
    این تابع الان هم از همان‌جا (بعد از پایان عادی run_scan_cycle) و هم از داخل
    مسیر revert کردن state در حلقه‌ی اصلی صدا زده می‌شود، تا در هر دو حالت، هیچ
    پرچمی که واقعاً روی دیسک/ریموت ثبت شده هرگز توسط یک revert محلی به عقب برنگردد."""
    try:
        disk_state = load_state()
        disk_windows = disk_state.get("tf_windows", {})
        if not disk_windows:
            return
        mem_windows = state.setdefault("tf_windows", {})
        for key, disk_val in disk_windows.items():
            mem_val = mem_windows.get(key)
            if disk_val and (not mem_val or disk_val >= mem_val):
                mem_windows[key] = disk_val
    except Exception as e:
        logger.warning(f"⚠️ خواندن پرچم‌های tf_windows از دیسک ناموفق بود: {e}")


def fetch_closed_klines_twelvedata(symbol: str, limit: int, interval: str, bar_seconds: int) -> List[Dict[str, Any]]:
    """کندل‌های بسته‌شده برای هر نماد و هر تایم‌فریمی از Twelve Data"""
    if not TWELVEDATA_API_KEY:
        return []
    now_ts = time.time()
    cache_key = f"{symbol}|{interval}"
    expiry = _TD_UNSUPPORTED.get(cache_key)
    if expiry and expiry > now_ts:
        return []  # قبلاً فهمیدیم این نماد/تایم‌فریم در Twelve Data نیست - بدون درخواست رد می‌شود
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
        # می‌تواند ۴۰۴ دائمی یا خطای شبکه/۴۲۹ نهایی باشد - برای احتیاط ۲۴ ساعت رد می‌شود
        _TD_UNSUPPORTED[cache_key] = now_ts + TD_UNSUPPORTED_TTL_SECONDS
        return []
    if data.get("status") == "error":
        logger.warning(f"⚠️ Twelve Data برای {symbol} ({interval}) خطا داد: {data.get('message')} "
                        f"(احتمالاً این نماد/تایم‌فریم نیاز به پلن پولی دارد)")
        _TD_UNSUPPORTED[cache_key] = now_ts + TD_UNSUPPORTED_TTL_SECONDS
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
            vol_raw = v.get("volume")
            try:
                vol = float(vol_raw) if vol_raw is not None else None
            except (TypeError, ValueError):
                vol = None
            candles.append({"open_time": open_time, "o": o, "h": h, "l": l, "c": c, "v": vol})
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
        "open_trade": None,
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
                       open_time: int, v: Optional[float] = None,
                       require_volume_trend: bool = False) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    پردازش یک کندل جدید بسته‌شده و به‌روزرسانی وضعیت - معادل دقیقِ اجرای بار-به-بارِ
    منطق اندیکاتور Pine Script (بدون هیچ تغییری در شرایط کندل سیگنال، فقط فیلتر
    ضد تکرارِ خودِ اسکریپت + فیلتر روند حجم (فقط وقتی require_volume_trend=True،
    یعنی فقط تایم‌فریم ۱ دقیقه‌ای - توضیح کامل بالای ONE_MINUTE_VOLUME_FILTER_ENABLED)
    روی خروجی اعمال شده است.
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

    # 🆕 فیلتر روند حجم (فقط تایم‌فریم ۱ دقیقه‌ای - توضیح کامل بالای تعریف
    # ONE_MINUTE_VOLUME_FILTER_ENABLED): میانگین حجم ONE_MINUTE_VOLUME_TREND_LOOKBACK
    # کندل اخیر (شامل همین کندل سیگنال) باید از میانگین همان تعداد کندل قبل از
    # آن‌ها بیشتر باشد - وگرنه سیگنال این کندل رد می‌شود. اگر تاریخچه‌ی حجم هنوز
    # کافی نیست یا داده‌ی حجم موجود نیست (fail-open)، فیلتر سیگنال را مسدود نمی‌کند.
    volume_trend_ok = True
    if require_volume_trend:
        lookback = max(2, ONE_MINUTE_VOLUME_TREND_LOOKBACK)
        vols_with_current = [bar.get("v") for bar in hist] + [v]
        window = vols_with_current[-2 * lookback:]
        if len(window) == 2 * lookback and all(x is not None for x in window):
            prior_avg = sum(window[:lookback]) / lookback
            recent_avg = sum(window[lookback:]) / lookback
            volume_trend_ok = recent_avg > prior_avg

    final_bull = raw_bull and state_ok_bull and cooldown_ok_bull and price_move_ok and volume_trend_ok
    final_bear = raw_bear and state_ok_bear and cooldown_ok_bear and price_move_ok and volume_trend_ok

    signal = None
    swing_window = hist[-SWING_LOOKBACK:] if hist else []
    if final_bull:
        s["bull_used_this_trend"] = True
        s["last_bull_bar_index"] = s["bar_index"]
        s["last_signal_price"] = c
        swing_low = min([b["l"] for b in swing_window]) if swing_window else l
        raw_sl = min(ema7_i, swing_low)  # فاصله بیشتر و محافظه‌کارانه‌تر از این دو
        buffer = SL_BUFFER_ATR_MULT * atr_i if atr_i else abs(c) * 0.001
        sl = raw_sl - buffer
        signal = {"side": "BUY", "confirmed": bool(bullish_engulf or bullish_pin), "price": c, "open_time": open_time, "sl": sl}
    if final_bear:
        s["bear_used_this_trend"] = True
        s["last_bear_bar_index"] = s["bar_index"]
        s["last_signal_price"] = c
        swing_high = max([b["h"] for b in swing_window]) if swing_window else h
        raw_sl = max(ema7_i, swing_high)
        buffer = SL_BUFFER_ATR_MULT * atr_i if atr_i else abs(c) * 0.001
        sl = raw_sl + buffer
        signal = {"side": "SELL", "confirmed": bool(bearish_engulf or bearish_pin), "price": c, "open_time": open_time, "sl": sl}

    hist.append({"o": o, "h": h, "l": l, "c": c, "ema7": ema7_i, "dt_ms": open_time, "v": v})
    s["hist"] = hist[-HIST_KEEP:]
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


# ================== مدیریت معامله باز (حد ضرر / ۴ تارگت / تریلینگ رانر) ==================

def open_new_trade(signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    entry = signal["price"]
    sl = signal["sl"]
    r = abs(entry - sl)
    if r <= 0:
        return None
    return {
        "side": signal["side"], "entry": entry, "sl": sl, "r": r,
        "working_sl": sl,                                    # حد ضرر «فعلی» - با هر تارگت جابه‌جا می‌شود
        "hit": {str(lvl["n"]): False for lvl in TARGET_LEVELS},
        "closed_frac": 0.0,                                  # چند درصد از کل پوزیشن تا الان بسته شده
        "trailing_activated": False,
        "peak_price": None,                                  # بالاترین (BUY) / پایین‌ترین (SELL) قیمت بعد از تارگت ۴
        "fills": [],                                          # [{"target":..,"frac":..,"r":..}] - مبنای دقیق final_r
        "closed": False,
    }


def compute_final_r(trade: Dict[str, Any]) -> float:
    """نتیجه‌ی نهایی معامله بر حسب R - مجموع دقیق «درصد بسته‌شده × R واقعی به‌دست‌آمده»
    در هر مرحله (trade["fills"])، نه یک فرمول تقریبی. اگر معامله هنوز هیچ fill ای
    نداشته باشد (نباید پیش بیاید مگر برای معامله‌ی هنوز باز)، صفر برمی‌گرداند."""
    return sum(f["frac"] * f["r"] for f in trade.get("fills", []))


def _target_hit_summary(trade: Dict[str, Any]) -> str:
    """خلاصه‌ی کوتاه اینکه کدام تارگت‌ها به دست آمدند و رانر چه سرنوشتی داشت -
    برای نمایش در پیام‌های خروج و گزارش روزانه."""
    hit_ns = [lvl["n"] for lvl in TARGET_LEVELS if trade["hit"].get(str(lvl["n"]))]
    if not hit_ns:
        parts = ["No targets reached"]
    else:
        parts = [f"T{n}" for n in hit_ns]
        parts = ["Targets hit: " + ", ".join(parts)]
    if trade.get("close_type") == "runner_stop":
        runner_fill = next((f for f in trade.get("fills", []) if f.get("target") == "runner"), None)
        if runner_fill:
            parts.append(f"Runner (final {RUNNER_FRACTION*100:.0f}%) closed at {runner_fill['r']:+.2f}R by trailing stop")
    elif trade["hit"].get("4") and trade.get("close_type") not in ("runner_stop", None):
        parts.append(f"Runner ({RUNNER_FRACTION*100:.0f}%) still open when this record was logged")
    return " · ".join(parts)


# ================== آمار عملکرد سیگنال‌ها ==================

def log_target_event(target_events: List[Dict[str, Any]], symbol: str, tf_key: str,
                      source_label: Optional[str], trade: Dict[str, Any], event: Dict[str, Any]) -> None:
    """
    طبق درخواست کاربر: تارگت‌های به‌دست‌آمده و بسته‌شدن رانر باید «به‌صورت لحظه‌ای
    دقیق و آپدیت در لحظه» ثبت شوند، نه فقط وقتی کل معامله در نهایت بسته می‌شود.
    این تابع همان لحظه که یک رویداد (تارگت/ریسک‌فری/استاپ/رانر) رخ می‌دهد صدا زده
    می‌شود - قبل از این، log_trade_result فقط در پایان معامله (بسته‌شدن کامل) صدا
    زده می‌شد؛ یعنی اگر یک معامله چند روز طول می‌کشید (مثلاً تارگت۱ دوشنبه، بسته‌شدن
    نهایی چهارشنبه)، گزارش روزانه‌ی دوشنبه اصلاً آن تارگت را نمی‌دید - همه‌چیز با
    تاریخ «بسته‌شدن نهایی» ثبت می‌شد. حالا هر رویداد با زمان واقعی وقوعش (نه زمان
    بسته‌شدن نهایی معامله) در state["target_events"] ذخیره می‌شود، و
    format_results_message از همین لاگ برای «تارگت‌های امروز» استفاده می‌کند - پس
    گزارش هر روز دقیقاً همان چیزی را نشان می‌دهد که واقعاً همان روز اتفاق افتاده.
    """
    if event.get("type") == "rr":
        ev_type = f"target_{event['level']}"
        r_value = event.get("r_multiple")
    else:
        ev_type = event.get("type", "stop")
        r_value = event.get("r")
    target_events.append({
        "symbol": symbol, "tf": tf_key, "tier": source_label or "واچ‌لیست اصلی",
        "side": trade.get("side"), "type": ev_type,
        "r": r_value, "frac": event.get("frac", 0.0),
        "at": datetime.now(timezone.utc).isoformat(),
    })
    del target_events[:-5000]  # جلوگیری از رشد بی‌نهایت state.json (مثل trade_history)


def log_trade_result(trade_history: List[Dict[str, Any]], symbol: str, tf_key: str,
                      source_label: Optional[str], trade: Dict[str, Any]) -> None:
    """وقتی معامله بسته می‌شود (استاپ/ریسک‌فری/تارگت‌های میانی/رانر/سیگنال مخالف)،
    نتیجه‌ی شفاف آن (چطور بسته شده + چند R + کدام تارگت‌ها گرفته شد + سرنوشت رانر)
    را برای آمار برد/باخت ذخیره می‌کند - چیزی که در گزارش روزانه‌ی خودکار کانال
    دیده می‌شود."""
    close_type = trade.get("close_type", "unknown")
    hit_targets = [lvl["n"] for lvl in TARGET_LEVELS if trade["hit"].get(str(lvl["n"]))]
    runner_fill = next((f for f in trade.get("fills", []) if f.get("target") == "runner"), None)
    trade_history.append({
        "symbol": symbol, "tf": tf_key, "tier": source_label or "واچ‌لیست اصلی",
        "side": trade["side"], "entry": trade["entry"], "sl": trade["sl"],
        "final_r": round(compute_final_r(trade), 4),
        "close_type": close_type,
        "close_reason": CLOSE_TYPE_LABELS_EN.get(close_type, close_type),
        "targets_hit": hit_targets,                          # مثلاً [1,2,3] - دقیقاً کدام تارگت‌ها گرفته شد
        "runner_closed": close_type == "runner_stop",
        "runner_r": round(runner_fill["r"], 4) if runner_fill else None,
        "closed_at": datetime.now(timezone.utc).isoformat(),
    })
    del trade_history[:-2000]  # جلوگیری از رشد بی‌نهایت state.json


def compute_stats(history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not history:
        return None
    total = len(history)
    wins = sum(1 for h in history if h.get("final_r", 0) > 0)
    breakeven = sum(1 for h in history if h.get("final_r", 0) == 0)
    losses = sum(1 for h in history if h.get("final_r", 0) < 0)
    total_r = sum(h.get("final_r", 0) for h in history)
    # چند معامله به هر تارگت رسیدند (صرف‌نظر از اینکه در نهایت چطور بسته شدند) +
    # چند رانر با موفقیت توسط تریلینگ‌استاپ بسته شد - دقیقاً چیزی که کاربر خواسته بود
    target_hit_counts = {lvl["n"]: sum(1 for h in history if lvl["n"] in h.get("targets_hit", [])) for lvl in TARGET_LEVELS}
    runner_closes = [h for h in history if h.get("runner_closed")]
    return {
        "total": total, "wins": wins, "breakeven": breakeven, "losses": losses,
        "total_r": total_r, "avg_r": total_r / total, "win_rate": wins / total * 100,
        "target_hit_counts": target_hit_counts,
        "runner_count": len(runner_closes),
        "runner_avg_r": (sum(h["runner_r"] for h in runner_closes if h.get("runner_r") is not None) / len(runner_closes))
                         if runner_closes else None,
    }


def compute_stats_by_group(history: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for h in history:
        groups.setdefault(h.get(key, "?"), []).append(h)
    return {name: compute_stats(items) for name, items in groups.items()}


def _todays_target_stats(target_events: List[Dict[str, Any]], today_str: str) -> Dict[str, Any]:
    """محاسبه‌ی «چند تارگت واقعاً امروز به‌دست آمد» و «چند رانر واقعاً امروز با
    تریلینگ بسته شد» - از روی state["target_events"] (که هر رویداد را با زمان
    وقوع واقعی‌اش ثبت می‌کند)، نه از روی trade_history (که فقط زمان بسته‌شدن
    نهایی معامله را دارد). این یعنی اگر معامله‌ای چند روز طول بکشد، تارگت‌هایی که
    امروز واقعاً زده شدند در گزارش امروز می‌آیند - حتی اگر خودِ معامله فردا یا
    پس‌فردا کامل بسته شود."""
    todays_events = [e for e in target_events if str(e.get("at", "")).startswith(today_str)]
    target_hit_counts = {lvl["n"]: 0 for lvl in TARGET_LEVELS}
    for e in todays_events:
        et = e.get("type", "")
        if et.startswith("target_"):
            try:
                n = int(et.split("_", 1)[1])
            except ValueError:
                continue
            if n in target_hit_counts:
                target_hit_counts[n] += 1
    runner_events = [e for e in todays_events if e.get("type") == "runner_stop"]
    runner_rs = [e["r"] for e in runner_events if e.get("r") is not None]
    return {
        "target_hit_counts": target_hit_counts,
        "runner_count": len(runner_events),
        "runner_avg_r": (sum(runner_rs) / len(runner_rs)) if runner_rs else None,
    }


def format_results_message(trade_history: List[Dict[str, Any]], now_utc: Optional[datetime] = None,
                            target_events: Optional[List[Dict[str, Any]]] = None,
                            flow_signals_today: int = 0) -> Optional[str]:
    """گزارش روزانه‌ی خودکار عملکرد سیگنال‌ها - فقط معاملاتی که «امروز» (بر اساس
    تاریخ UTC) بسته شده‌اند، نه کل تاریخچه (که قبلاً باعث می‌شد گزارش هر روز
    بزرگ‌تر و کمتر خوانا شود). اگر امروز هیچ معامله‌ای بسته نشده و هیچ تارگت/رانری
    هم امروز رخ نداده (و هیچ سیگنال FLOW هم پست نشده)، None برمی‌گرداند (چیزی برای
    گزارش نیست).

    طبق درخواست کاربر: تارگت‌ها و بسته‌شدن رانر باید دقیق و «همان روزی که واقعاً
    اتفاق افتادند» گزارش شوند - نه فقط بر اساس تاریخ بسته‌شدن نهایی معامله. برای
    همین «چند معامله کامل امروز بسته شد» (برد/باخت/R - که ذاتاً فقط در لحظه‌ی
    بسته‌شدن معلوم می‌شود) از trade_history می‌آید، ولی «امروز چند تارگت خورد /
    چند رانر بسته شد» از state["target_events"] (لاگ لحظه‌ای) محاسبه می‌شود.

    flow_signals_today: تعداد سیگنال‌های FLOW (جریان استیبل‌کوین) که امروز پست شدند
    (state["flow_signals_today"] در main). این‌ها Entry/Stop ندارند و ذاتاً قابل
    ردیابیِ R-based نیستند، پس در برد/باخت/By symbol/By timeframe حساب نمی‌شوند -
    ولی طبق درخواست کاربر که «تمام سیگنال‌های صادرشده» در گزارش روزانه شناسایی شوند،
    حداقل تعدادشان به‌عنوان یک خط اطلاعاتی جداگانه اضافه می‌شود، تا هیچ سیگنالِ
    پست‌شده‌ای کاملاً از قلم نیفتد."""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    todays = [h for h in trade_history if str(h.get("closed_at", "")).startswith(today_str)]
    stats = compute_stats(todays)
    today_target_stats = _todays_target_stats(target_events or [], today_str)

    if (not stats and today_target_stats["runner_count"] == 0
            and not any(today_target_stats["target_hit_counts"].values()) and not flow_signals_today):
        return None

    by_symbol = compute_stats_by_group(todays, "symbol")
    symbol_lines = [
        f"  {name}: {s['total']} trades · {s['win_rate']:.0f}% win · {s['total_r']:+.2f}R"
        for name, s in sorted(by_symbol.items(), key=lambda kv: -kv[1]["total"])[:15]
    ]

    tf_order = ["5m", "15m", "1h"]
    by_tf = compute_stats_by_group(todays, "tf")
    tf_lines = [
        f"  {name}: {s['total']} trades · {s['win_rate']:.0f}% win · {s['total_r']:+.2f}R"
        for name, s in sorted(by_tf.items(), key=lambda kv: tf_order.index(kv[0]) if kv[0] in tf_order else 99)
    ]

    target_lines = [
        f"  T{lvl['n']} ({lvl['r']}R, {lvl['close_frac']*100:.0f}%): {today_target_stats['target_hit_counts'][lvl['n']]} reached"
        for lvl in TARGET_LEVELS
    ]
    runner_line = (
        f"  Runner (final {RUNNER_FRACTION*100:.0f}%, trailing {TRAILING_DISTANCE_R}R): "
        f"{today_target_stats['runner_count']} closed" +
        (f" · avg {today_target_stats['runner_avg_r']:+.2f}R" if today_target_stats['runner_avg_r'] is not None else "")
    )

    closed_summary = (
        f"✅ Wins: {stats['wins']} ({stats['win_rate']:.0f}%)\n"
        f"⚪ Breakeven: {stats['breakeven']}\n"
        f"❌ Losses: {stats['losses']}\n\n"
        f"Total: <b>{stats['total_r']:+.2f}R</b>\n"
        f"Average per trade: <b>{stats['avg_r']:+.2f}R</b>\n\n"
        f"<b>By symbol</b>\n" + "\n".join(symbol_lines) + "\n\n"
        f"<b>By timeframe</b>\n" + "\n".join(tf_lines) + "\n\n"
    ) if stats else "<i>No trades fully closed today yet (some may still be open with targets already hit below).</i>\n\n"

    trades_count = stats["total"] if stats else 0
    flow_line = f"\n\n🔵 Stablecoin flow alerts posted today: {flow_signals_today}" if flow_signals_today else ""
    return (
        f"📊 DAILY SIGNAL PERFORMANCE — {today_str}\n"
        f"<i>{trades_count} trades closed today - every signal sent, no cherry-picking</i>\n\n"
        f"{closed_summary}"
        f"<b>Targets reached today</b>\n" + "\n".join(target_lines) + "\n" + runner_line + flow_line
    )


def check_open_trade(trade: Dict[str, Any], candle: Dict[str, Any], ema7_now: Optional[float]) -> List[Dict[str, Any]]:
    """
    وضعیت معامله‌ی باز را با یک کندل تازه‌بسته‌شده چک می‌کند - این نسخه‌ی مبتنی بر
    high/low کندل است. علاوه بر این، حالا (بعد از تبدیل به پروسه‌ی پیوسته) یک مسیر
    مکمل هم هست: check_open_trade_live که با یک قیمت لحظه‌ای (نه کل کندل) کار می‌کند
    و از WebSocket/REST زنده صدا زده می‌شود - برای واچ‌لیست آلت‌کوین از WebSocket
    (لحظه‌ای، بدون هزینه). این یعنی هر معامله‌ی باز حداقل از دو مسیر مستقل رصد
    می‌شود؛ نتیجه‌ی هر بسته‌شدن (تارگت/استاپ/ریسک‌فری/رانر/سیگنال مخالف) همیشه به‌صورت
    close_type دقیق ثبت و در پیام (خط Result) و در trade_history گزارش داده می‌شود.

    منطق (طبق RiskRivard_System.py ارسالی توسط کاربر):
      - قبل از تارگت ۱: اگر قیمت به حد ضرر اصلی برسد -> بستن ۱۰۰٪ با ضرر کامل (-1R).
      - تارگت ۱ (۱R): بستن ۲۰٪ + انتقال حد ضرر به Entry (ریسک‌فری).
      - تارگت ۲ (۲R): بستن ۳۰٪ (جمعاً ۵۰٪) + انتقال حد ضرر به سطح تارگت ۱.
      - تارگت ۳ (۴R): بستن ۱۵٪ (جمعاً ۶۵٪) + انتقال حد ضرر به سطح تارگت ۲.
      - تارگت ۴ (۶R): بستن ۱۰٪ (جمعاً ۷۵٪) + فعال‌شدن تریلینگ روی ۲۵٪ باقی‌مانده (رانر).
      - بعد از تارگت ۴: دیگر حد ضرر معمولی چک نمی‌شود - فقط تریلینگ رانر: اگر قیمت
        TRAILING_DISTANCE_R (۱.۵R) از بالاترین/پایین‌ترین قیمت رسیده عقب‌نشینی کند،
        رانر با هر R واقعی که در آن لحظه به‌دست آمده بسته می‌شود.
    """
    if trade is None or trade.get("closed"):
        return []
    events = []
    side, entry, r = trade["side"], trade["entry"], trade["r"]
    sign = 1 if side == "BUY" else -1

    def realized_r(price: float) -> float:
        return (price - entry) / r * sign

    # ---- فاز رانر: بعد از تارگت ۴، فقط تریلینگ‌استاپ چک می‌شود ----
    if trade["trailing_activated"]:
        extreme = candle["h"] if side == "BUY" else candle["l"]
        if trade["peak_price"] is None:
            trade["peak_price"] = extreme
        elif side == "BUY":
            trade["peak_price"] = max(trade["peak_price"], extreme)
        else:
            trade["peak_price"] = min(trade["peak_price"], extreme)

        trail_stop = trade["peak_price"] - sign * TRAILING_DISTANCE_R * r
        hit_trail = (candle["l"] <= trail_stop) if side == "BUY" else (candle["h"] >= trail_stop)
        if hit_trail:
            runner_frac = max(0.0, 1.0 - trade["closed_frac"])
            runner_r = realized_r(trail_stop)
            trade["fills"].append({"target": "runner", "frac": runner_frac, "r": runner_r})
            trade["closed_frac"] = 1.0
            trade["closed"] = True
            trade["close_type"] = "runner_stop"
            events.append({"type": "runner_stop", "price": trail_stop, "r": runner_r, "frac": runner_frac})
        return events

    # ---- حد ضرر معمولی (فقط قبل از تارگت ۴ - بعد از آن جای خودش را به تریلینگ می‌دهد) ----
    sl = trade["working_sl"]
    hit_sl = (candle["l"] <= sl) if side == "BUY" else (candle["h"] >= sl)
    if hit_sl:
        remaining = max(0.0, 1.0 - trade["closed_frac"])
        exit_r = realized_r(sl)
        trade["fills"].append({"target": "stop", "frac": remaining, "r": exit_r})
        trade["closed_frac"] = 1.0
        trade["closed"] = True
        if trade["hit"]["3"]:
            trade["close_type"] = "stop_at_t2"
        elif trade["hit"]["2"]:
            trade["close_type"] = "stop_at_t1"
        elif trade["hit"]["1"]:
            trade["close_type"] = "breakeven"
        else:
            trade["close_type"] = "stop"
        events.append({"type": trade["close_type"], "price": sl, "r": exit_r, "frac": remaining})
        return events

    # ---- تارگت‌ها به ترتیب (ممکن است در یک کندل بزرگ چند تارگت پشت‌سرهم رد شوند) ----
    for lvl in TARGET_LEVELS:
        key = str(lvl["n"])
        if trade["hit"][key]:
            continue
        target_price = entry + sign * lvl["r"] * r
        reached = (candle["h"] >= target_price) if side == "BUY" else (candle["l"] <= target_price)
        if not reached:
            continue
        trade["hit"][key] = True
        trade["closed_frac"] += lvl["close_frac"]
        trade["fills"].append({"target": lvl["n"], "frac": lvl["close_frac"], "r": lvl["r"]})
        events.append({"type": "rr", "level": lvl["n"], "r_multiple": lvl["r"],
                        "price": target_price, "frac": lvl["close_frac"]})
        if lvl["sl_after"] == "entry":
            trade["working_sl"] = entry
        elif lvl["sl_after"] == "t1":
            trade["working_sl"] = entry + sign * TARGET_LEVELS[0]["r"] * r
        elif lvl["sl_after"] == "t2":
            trade["working_sl"] = entry + sign * TARGET_LEVELS[1]["r"] * r
        if lvl.get("activates_trailing"):
            trade["trailing_activated"] = True
            trade["peak_price"] = candle["h"] if side == "BUY" else candle["l"]

    return events


STOP_CONFIRM_SECONDS = 8  # فاصله‌ی زمانی لازم بین اولین تیک عبور از استاپ و تایید نهایی (دقیقاً مثل candle_engine.py)


def check_open_trade_live(trade: Dict[str, Any], live_price: float, symbol_for_log: str = "?") -> List[Dict[str, Any]]:
    """نسخه‌ی سبک check_open_trade که فقط یک قیمت لحظه‌ای (نه کندل کامل) دارد - برای
    رهگیری زنده‌ی معامله‌ی باز بین دو بسته‌شدن کندل (از WebSocket). منطق (تارگت/
    استاپ/ریسک‌فری/تریلینگ رانر) دقیقاً همان check_open_trade است، فقط به‌جای
    high/low از یک نقطه‌ی قیمتی استفاده می‌کند.

    ⚠️ محافظ «استاپ کاذب» (عیناً از candle_engine.py): برخورد به استاپ بلافاصله
    نهایی نمی‌شود - باید در دو تیک متوالی با حداقل STOP_CONFIRM_SECONDS ثانیه فاصله،
    هر دو بار قیمت هنوز از استاپ عبور کرده باشد تا واقعاً بسته شود. تارگت‌ها (که فقط
    سود را قفل می‌کنند، ریسکی ندارند) فوری و بدون تاخیر ثبت می‌شوند - این محافظ فقط
    برای استاپ (trailing_activated=False) اعمال می‌شود؛ تریلینگ‌استاپ رانر بعد از
    تارگت ۴ عمداً از این تاخیر مستثنی است چون خودش از قبل با فاصله‌ی ۱.۵R یک بافر
    طبیعی در برابر نوسان لحظه‌ای دارد."""
    side, sl = trade["side"], trade.get("working_sl") if not trade.get("trailing_activated") else None
    would_stop = False
    if not trade.get("trailing_activated") and sl is not None:
        would_stop = (side == "BUY" and live_price <= sl) or (side == "SELL" and live_price >= sl)

    if not would_stop:
        trade.pop("_stop_pending_since", None)
        fake_candle = {"o": live_price, "h": live_price, "l": live_price, "c": live_price}
        return check_open_trade(trade, fake_candle, ema7_now=None)

    now = time.time()
    pending_since = trade.get("_stop_pending_since")
    if pending_since is None:
        trade["_stop_pending_since"] = now
        logger.info(f"⏳ تیک احتمالی برخورد به استاپ برای {symbol_for_log} — منتظر تیک تاییدی قبل "
                    f"از بستن معامله (جلوگیری از بستن اشتباه با یک تیک بد)")
        return []
    if now - pending_since < STOP_CONFIRM_SECONDS:
        return []

    trade.pop("_stop_pending_since", None)
    fake_candle = {"o": live_price, "h": live_price, "l": live_price, "c": live_price}
    return check_open_trade(trade, fake_candle, ema7_now=None)


def fetch_live_price(symbol: str) -> Optional[float]:
    """قیمت لحظه‌ای (نه کندل بسته‌شده) از اندپوینت سبک /price - هزینه‌اش هم مثل بقیه
    ۱ کردیت است، پس فقط برای معاملات باز (نه هر نماد) و با سقف MAX_LIVE_CHECKS_PER_RUN
    صدا زده می‌شود."""
    if not TWELVEDATA_API_KEY:
        return None
    url = f"{TWELVEDATA_BASE}/price"
    params = {"symbol": symbol, "apikey": TWELVEDATA_API_KEY}
    data = retry_request("GET", url, params=params)
    if not data or not isinstance(data, dict) or "price" not in data:
        return None
    try:
        return float(data["price"])
    except (TypeError, ValueError):
        return None


def build_chart_from_hist(hist: List[Dict[str, Any]], title: str,
                           trade: Optional[Dict[str, Any]] = None,
                           live_price: Optional[float] = None) -> Optional[str]:
    if len(hist) < 15:
        return None
    hist_to_plot = list(hist)
    if live_price:
        # یک کندل لحظه‌ای اضافه می‌شود تا آخرین نقطه‌ی چارت دقیقاً همان قیمتی باشد
        # که باعث فعال‌شدن این رویداد (تارگت/استاپ) شده، نه آخرین کندل بسته‌شده که
        # ممکن است چند دقیقه قدیمی‌تر باشد.
        hist_to_plot.append({
            "o": live_price, "h": live_price, "l": live_price, "c": live_price,
            "ema7": hist_to_plot[-1]["ema7"], "dt_ms": int(time.time() * 1000),
        })
    df = pd.DataFrame(hist_to_plot)
    df["dt"] = pd.to_datetime(df["dt_ms"], unit="ms", utc=True)
    df = df.set_index("dt")
    ohlc = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close"})[["open", "high", "low", "close"]]
    apds = [mpf.make_addplot(df["ema7"], color="dodgerblue", width=1)]

    hlines_vals, hlines_colors, labels = [], [], []
    if trade:
        sign = 1 if trade["side"] == "BUY" else -1
        hlines_vals.append(trade["entry"]); hlines_colors.append("blue"); labels.append(("Entry", trade["entry"], "blue"))
        hlines_vals.append(trade["sl"]); hlines_colors.append("red"); labels.append(("Stop", trade["sl"], "red"))
        # 🔴 رفع باگ: قبلاً این حلقه روی RR_TARGETS (یعنی خودِ مقادیر R = [1,2,4,6])
        # می‌چرخید و از همان عدد R برای لیبل «T{}» استفاده می‌کرد - یعنی تارگت سوم
        # (که ۴R پاداش دارد) روی چارت «T4» و تارگت چهارم (که ۶R پاداش دارد) «T6»
        # نوشته می‌شد؛ به‌جای شماره‌ی واقعی تارگت (۱ تا ۴)، مقدار پاداش (R) جای آن
        # نوشته می‌شد. الان روی خودِ TARGET_LEVELS می‌چرخد و از lvl["n"] (شماره‌ی
        # واقعی تارگت) برای لیبل و از lvl["r"] فقط برای محاسبه‌ی قیمت استفاده می‌کند.
        for lvl_cfg in TARGET_LEVELS:
            lvl = trade["entry"] + sign * lvl_cfg["r"] * trade["r"]
            hlines_vals.append(lvl); hlines_colors.append("green")
            labels.append((f"T{lvl_cfg['n']}", lvl, "green"))

    try:
        plot_kwargs = dict(type="candle", style="charles", addplot=apds, title=title, volume=False, returnfig=True)
        if hlines_vals:
            plot_kwargs["hlines"] = dict(hlines=hlines_vals, colors=hlines_colors, linestyle="--", linewidths=0.8)
        fig, axlist = mpf.plot(ohlc, **plot_kwargs)
        ax = axlist[0]
        x_right = len(ohlc) - 1
        if live_price:
            ax.annotate("Live", xy=(x_right, live_price), xytext=(5, 12), textcoords="offset points",
                        color="darkorange", fontsize=8, va="center", fontweight="bold")
            ax.scatter([x_right], [live_price], color="darkorange", s=25, zorder=5)
        for name, val, color in labels:
            ax.annotate(name, xy=(x_right, val), xytext=(5, 0), textcoords="offset points",
                        color=color, fontsize=8, va="center", fontweight="bold")
        fig.savefig(CHART_PATH, dpi=100, bbox_inches="tight")
        import matplotlib.pyplot as plt
        plt.close(fig)
        return CHART_PATH
    except Exception as e:
        logger.warning(f"⚠️ ساخت چارت ناموفق بود: {e}")
        return None


def send_trade_exit(bot: "TradeiscoolBot", symbol: str, display_name: str, tf_key: str, tf_label: str,
                     trade: Dict[str, Any], event: Dict[str, Any], hist: List[Dict[str, Any]],
                     trade_history: Optional[List[Dict[str, Any]]] = None,
                     source_label: Optional[str] = None, live_price: Optional[float] = None,
                     target_events: Optional[List[Dict[str, Any]]] = None,
                     global_state: Optional[Dict[str, Any]] = None) -> int:
    """پیام خروج (تارگت/استاپ/ریسک‌فری/رانر) رو می‌فرسته و در صورت بسته‌شدن معامله،
    نتیجه رو ثبت می‌کنه. هم از مسیر کندلی (process_and_send_symbol_tf) و هم از مسیر
    قیمت زنده (WebSocket live) صدا زده می‌شه - یک نقطه‌ی مشترک، بدون تکرار منطق.

    طبق درخواست کاربر: این پیام‌ها باید روی پیام سیگنال ورود مربوطه ریپلای بزنند
    (reply_to_message_id) - همان چیزی که از قبل هم بود؛ فقط دیگر خودشان عکس چارت
    جدیدی ندارند (photo_path=None → فقط متن). thumbnail کوچکی که تلگرام در پیش‌نمایش
    ریپلای نشان می‌دهد، خودِ عکسِ چارتِ همان پیام ورود اصلی است (رفتار native خودِ
    تلگرام)، نه یک عکس تازه که ربات بفرستد."""
    if event["type"] == "rr":
        msg = format_rr_exit_message(symbol, display_name, tf_label, trade, event, source_label)
    else:
        msg = format_exit_message(symbol, display_name, tf_label, trade, event, source_label)
    reply_id = trade.get("signal_message_id")
    sent = 0
    if bot.send_telegram_photo(msg, None, reply_to_message_id=reply_id):
        logger.info(f"📤 خروج ارسال شد: {display_name} [{tf_key}] {event['type']}"
                    f"{' (زنده)' if live_price else ''}")
        sent = 1
    # 🆕 قطع خودکار سیگنال بعد از استاپ‌های پیاپی: فقط استاپ لاس *واقعی* (ضرر خالص
    # قبل از هر تارگتی - close_type == "stop") حساب می‌شود، نه ریسک‌فری/تارگت‌های
    # بعدی که همه سود قفل‌شده هستند.
    if global_state is not None and event.get("type") == "stop":
        record_stop_loss(display_name, global_state)
    # ثبت لحظه‌ای این رویداد (تارگت/ریسک‌فری/استاپ/رانر) - مستقل از اینکه کل معامله
    # همین الان بسته شده باشد یا نه - تا گزارش روزانه دقیقاً همان روزی که رویداد
    # واقعاً رخ داده آن را نشان دهد.
    if target_events is not None:
        log_target_event(target_events, symbol, tf_key, source_label, trade, event)
    if trade.get("closed") and trade_history is not None:
        log_trade_result(trade_history, symbol, tf_key, source_label, trade)
    return sent


def _td_unsupported_warning_line(symbol: str) -> Optional[str]:
    """اگر این نماد (فرمت TICKER/USD) قبلاً حداقل یک بار توسط fetch_closed_klines_twelvedata
    به‌عنوان «پشتیبانی‌نشده در Twelve Data» کش شده باشد (هر تایم‌فریمی، چون کلید کش به‌صورت
    "{symbol}|{interval}" است)، یک خط هشدار انگلیسی برمی‌گرداند تا در متن پیام کانال دیده شود.

    چرا لازم است: اگر ادمین این سیگنال را برای رهگیری خودکار به ربات دوم فوروارد کند
    (subscription_bot.py تشخیص می‌دهد → candle_engine.py صف می‌کند)، رهگیری زنده‌ی آن‌جا
    (fetch_live_price/fetch_closed_klines) کاملاً به همین Twelve Data وابسته است - نمادی که
    این‌جا پشتیبانی نشده، آن‌جا هم پشتیبانی نمی‌شود؛ معامله باز می‌ماند ولی هیچ‌وقت تارگت/استاپش
    رهگیری نمی‌شود (بی‌سروصدا، بدون خطای قابل‌مشاهده). این خط از قبل به ادمین هشدار می‌دهد که
    این سیگنال خاص را فوروارد نکند. نمادهای واچ‌لیست اصلی (SOL/XRP/ADA) عملاً
    هیچ‌وقت این کش را نمی‌گیرند (آلت‌کوین‌های پرحجم، همیشه پشتیبانی می‌شوند)، پس این
    خط معمولاً فقط روی سیگنال‌های ۳۰ ارز برتر/ارزهای پرتحرک ظاهر می‌شود."""
    prefix = f"{symbol}|"
    if any(k.startswith(prefix) for k in _TD_UNSUPPORTED):
        return "⚠️ Not tracked on Twelve Data — please don't forward this one for auto-tracking."
    return None


def process_and_send_symbol_tf(bot: "TradeiscoolBot", fetch_fn, symbol: str, display_name: str,
                                tf_key: str, tf_cfg: Dict[str, Any], sym_state: Optional[Dict[str, Any]],
                                trade_history: Optional[List[Dict[str, Any]]] = None,
                                source_label: Optional[str] = None,
                                target_events: Optional[List[Dict[str, Any]]] = None,
                                global_state: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], int, Optional[str]]:
    """
    نسخه‌ی کامل: علاوه بر تشخیص سیگنال، معامله‌ی باز را هم (با حد ضرر/۴ تارگت/تریلینگ/
    رانر - عیناً منطق candle_engine.py) ردیابی می‌کند و برای هر رویداد (ورود، هر تارگت،
    استاپ، ریسک‌فری، تریلینگ، رانر، بسته‌شدن اجباری با سیگنال مخالف) پیام همراه با عکس
    چارت می‌فرستد؛ پیام‌های خروج روی پیام سیگنال ورود ریپلای می‌زنند.
    global_state (اختیاری): دیکشنری کلی state.json - فقط برای چک «آیا این نماد امروز
    قفل شده» (is_symbol_blocked_today) قبل از باز کردن معامله‌ی جدید استفاده می‌شود؛
    مدیریت معامله‌ی از قبل باز، صرف‌نظر از قفل‌بودن، همچنان ادامه پیدا می‌کند.
    برمی‌گرداند: (state_جدید یا None اگر شکست خورد, تعداد پیام ارسالی, پیام خطا یا None)
    """
    tf_label = tf_cfg["label"]
    bar_seconds = tf_cfg["bar_seconds"]
    sent = 0

    def _try_open_trade(sig):
        """قبل از باز کردن معامله‌ی جدید، چک می‌کند این نماد امروز به‌خاطر استاپ‌های
        پیاپی قفل نشده باشد (MAX_DAILY_STOPS_PER_SYMBOL). اگر قفل بود، سیگنال کاملاً
        نادیده گرفته می‌شود (نه ارسال، نه ثبت معامله)."""
        if global_state is not None and is_symbol_blocked_today(display_name, global_state):
            logger.info(f"⛔ سیگنال {display_name} [{tf_key}] نادیده گرفته شد - امروز قبلاً "
                        f"{MAX_DAILY_STOPS_PER_SYMBOL}+ بار استاپ لاس واقعی خورده")
            return None
        return open_new_trade(sig)

    def _send_entry(sig, trade, hist):
        nonlocal sent
        chart = build_chart_from_hist(hist, f"{display_name} {tf_label} · Entry", trade=trade)
        # ⚠️ طبق درخواست صریح کاربر، هشدار «Not tracked on Twelve Data» دیگر به انتهای
        # پیام سیگنال اضافه نمی‌شود (_td_unsupported_warning_line دیگر اینجا صدا زده
        # نمی‌شود؛ خودِ تابع و کش _TD_UNSUPPORTED هنوز برای لاگ داخلی/تشخیصی نگه داشته
        # شده‌اند، فقط دیگر در متن پیام کانال ظاهر نمی‌شوند). به همین ترتیب، دیگر هیچ
        # خط اضافه‌ای (مثل «عملکرد نسبی به BTC») هم به پیام افزوده نمی‌شود - طبق
        # درخواست کاربر، فرمت پیام باید دقیقاً مطابق تصویر نمونه (فقط Entry/Stop/
        # تارگت‌ها/تاریخ) باشد.
        msg = format_entry_message(symbol, display_name, tf_label, sig, trade, bar_seconds=bar_seconds)
        msg_id = bot.send_telegram_photo(msg, chart)
        if msg_id:
            trade["signal_message_id"] = msg_id
            logger.info(f"📤 سیگنال ورود ارسال شد: {display_name} [{tf_key}] {sig['side']}")
            sent += 1
        time.sleep(1.5)

    def _send_exit(trade, event, hist):
        nonlocal sent
        sent += send_trade_exit(bot, symbol, display_name, tf_key, tf_label, trade, event, hist,
                                 trade_history=trade_history, source_label=source_label,
                                 target_events=target_events, global_state=global_state)
        time.sleep(1.5)

    def _force_close(trade, candle, hist):
        nonlocal sent
        exit_price = candle["c"]
        sign = 1 if trade["side"] == "BUY" else -1
        remaining = max(0.0, 1.0 - trade["closed_frac"])
        exit_r = (exit_price - trade["entry"]) / trade["r"] * sign
        trade["fills"].append({"target": "opposite_signal", "frac": remaining, "r": exit_r})
        trade["closed_frac"] = 1.0
        trade["closed"] = True
        trade["close_type"] = "opposite_signal"
        # طبق درخواست کاربر: پیام‌های خروج/بسته‌شدن دیگر عکس چارت ندارند - فقط متن
        # (دلیل کامل در کامنت بالای send_trade_exit).
        msg = format_forced_close_message(symbol, display_name, tf_label, trade, exit_price, source_label)
        reply_id = trade.get("signal_message_id")
        if bot.send_telegram_photo(msg, None, reply_to_message_id=reply_id):
            logger.info(f"📤 بسته‌شدن اجباری ارسال شد: {display_name} [{tf_key}] (سیگنال مخالف)")
            sent += 1
        if target_events is not None:
            log_target_event(target_events, symbol, tf_key, source_label, trade,
                              {"type": "opposite_signal", "frac": remaining, "r": exit_r})
        if trade_history is not None:
            log_trade_result(trade_history, symbol, tf_key, source_label, trade)
        time.sleep(1.5)

    if sym_state is None:
        candles = fetch_fn(symbol, BOOTSTRAP_LIMIT)
        if len(candles) < 30:
            return None, 0, "داده‌ی کافی برای گرم‌کردن اولیه دریافت نشد"
        state = new_candle_state()
        last_idx = len(candles) - 1
        for idx, k in enumerate(candles):
            state, sig = step_candle_state(state, k["o"], k["h"], k["l"], k["c"], k["open_time"],
                                            v=k.get("v"), require_volume_trend=(tf_key == "1m" and ONE_MINUTE_VOLUME_FILTER_ENABLED))
            if idx == last_idx and sig:
                trade = _try_open_trade(sig)
                if trade:
                    state["open_trade"] = trade
                    _send_entry(sig, trade, state["hist"])
        return state, sent, None
    else:
        state = _ensure_candle_state_fields(sym_state)
        last_open_time = state.get("last_open_time")
        # 🔴 رفع باگ «تارگت/استاپ خیلی از سیگنال‌ها اصلاً گزارش نمی‌شود»: قبلاً همیشه
        # فقط catchup_limit ثابت (۱۰) کندل آخر خوانده می‌شد. برای گروه‌های کم‌فرکانس
        # (۳۰ ارز برتر / ارزهای همسو با روند در تایم‌فریم‌های REST که هر چند ساعت
        # یک‌بار چک می‌شوند)، اگر بین دو بار چک بیش از ۱۰ کندل شکل گرفته باشد،
        # Twelve Data فقط «۱۰ تای آخر» را برمی‌گرداند - کندل‌های وسط (و هر تارگت/
        # استاپی که داخلشان اتفاق افتاده) هرگز حتی دیده نمی‌شوند، چون اصلاً در
        # پاسخ API نیستند (نه اینکه فقط دیر پردازش شوند - برای همیشه گم می‌شوند).
        # الان به‌جای عدد ثابت، دقیقاً همان تعداد کندلی که واقعاً از آخرین بار
        # سپری شده محاسبه و درخواست می‌شود (سقف ۱۰۰۰ برای احتیاط) - چون Twelve
        # Data برای time_series صرف‌نظر از outputsize فقط ۱ کردیت مصرف می‌کند،
        # این تغییر هیچ هزینه‌ی سهمیه‌ی اضافه‌ای ندارد.
        limit = tf_cfg.get("catchup_limit", 10)
        if last_open_time is not None:
            elapsed_ms = (time.time() * 1000) - last_open_time
            elapsed_bars = int(elapsed_ms / (bar_seconds * 1000)) + 2  # +۲ برای احتیاط
            limit = max(limit, min(elapsed_bars, 1000))
        candles = fetch_fn(symbol, limit)
        new_candles = [k for k in candles if last_open_time is None or k["open_time"] > last_open_time]
        last_new_idx = len(new_candles) - 1

        # ⚠️ رفع باگ «پیام تارگت/استاپ بلافاصله بعد از خود سیگنال»: این تایم‌فریم‌ها با
        # تاخیر چک می‌شوند (هر ۱۵ دقیقه، نه هر کندل) - یعنی وقتی بالاخره چک می‌شود،
        # ممکن است چند کندل بسته‌شده‌ی پشت‌سرهم یک‌جا در new_candles برسند. اگر سیگنالی
        # روی یکی از کندل‌های قدیمی‌تر همین دسته پیدا شود (نه آخرین کندل)، دیگر معامله
        # را همین‌جا باز نمی‌کنیم - چون بلافاصله در همان حلقه، کندل‌های بعدیِ این دسته
        # (که در واقعیت خودشان هم قبلاً بسته شده بودند) با تارگت/استاپش چک می‌شوند و
        # پیام «سیگنال جدید» و «تارگت/استاپ خورد» تقریباً پشت‌سرهم می‌رسند - عملاً سیگنال
        # را بی‌فایده می‌کند چون کاربر زمان واکنش ندارد. فقط سیگنالِ روی نزدیک‌ترین کندل
        # به «الان» پست می‌شود؛ سیگنال‌های قدیمی‌تر همین دسته صرفاً برای به‌روزرسانی
        # اندیکاتور/معامله‌ی باز پردازش می‌شوند، نه باز کردن معامله‌ی جدید (دقیقاً همان
        # رفتار candle_engine.py برای بیت‌کوین/اتریوم).
        for idx, k in enumerate(new_candles):
            state, sig = step_candle_state(state, k["o"], k["h"], k["l"], k["c"], k["open_time"],
                                            v=k.get("v"), require_volume_trend=(tf_key == "1m" and ONE_MINUTE_VOLUME_FILTER_ENABLED))
            ema7_now = state.get("ema7")

            if state.get("open_trade"):
                events = check_open_trade(state["open_trade"], k, ema7_now)
                for ev in events:
                    _send_exit(state["open_trade"], ev, state["hist"])

            if sig and idx == last_new_idx:
                prev_trade = state.get("open_trade")
                if prev_trade and not prev_trade.get("closed"):
                    _force_close(prev_trade, k, state["hist"])
                trade = _try_open_trade(sig)
                if trade:
                    state["open_trade"] = trade
                    _send_entry(sig, trade, state["hist"])

        return state, sent, None


# متن اقدام برای هر تارگت - شامل درصد دقیق بسته‌شده (از کل پوزیشن، نه از باقی‌مانده)
# و اینکه حد ضرر به کجا منتقل می‌شود، تا کاربر دقیقاً بداند بعد از هر پیام چه کاری
# روی پوزیشنش انجام دهد. طبق تصویر/کد RiskRivard_System ارسالی کاربر.
# طبق درخواست کاربر: عبارت‌بندی این خط‌ها دقیقاً مطابق تصاویر نمونه‌ی ارسالی
# («✂️ Close X% of your total position here, and move your stop-loss up to
# Target N.») بازنویسی شد.
TARGET_ACTION_LINE = {
    1: "✂️ Close 20% of your total position here, and move your stop-loss to entry (risk-free).",
    2: "✂️ Close 30% of your total position here, and move your stop-loss up to Target 1.",
    3: "✂️ Close 15% of your total position here, and move your stop-loss up to Target 2.",
    4: ("✂️ Close 10% of your total position here. The final 25% (\"Runner\") stays open with "
        "a trailing stop 1.5R behind the peak — no fixed exit, so a bigger move keeps paying."),
}


def _clean_symbol_label(symbol: str, display_name: str) -> str:
    """نام کوتاه انگلیسی برای نمایش در متن سیگنال (مطابق تصویر: «SOL» نه «SOL/USD»)."""
    return display_name.upper() if display_name else symbol


def _target_price(entry: float, sl: float, side: str, level_r: float) -> float:
    """قیمت دقیق هر تارگت را از روی entry/stop/جهت معامله محاسبه می‌کند - دقیقاً همان
    فرمولی که check_open_trade برای تشخیص برخورد به تارگت استفاده می‌کند، تا عددی که
    در پیام ورود (Target 1..4) نمایش داده می‌شود همیشه با منطق واقعی معامله یکی باشد."""
    r_unit = abs(entry - sl)
    sign = 1 if side == "BUY" else -1
    return entry + sign * level_r * r_unit


def format_entry_message(symbol: str, display_name: str, timeframe_label: str, sig: Dict[str, Any],
                          trade: Dict[str, Any], bar_seconds: int = 0) -> str:
    formed_ts = datetime.fromtimestamp(sig["open_time"] / 1000 + bar_seconds, tz=timezone.utc)
    ts = formed_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    emoji = "🟢" if sig["side"] == "BUY" else "🔴"
    side_label = "LONG" if sig["side"] == "BUY" else "SHORT"
    label = _clean_symbol_label(symbol, display_name)
    tf_en = timeframe_label

    # طبق درخواست کاربر: فرمت پیام ورود دقیقاً مطابق تصویر نمونه باشد - فقط
    # Entry/Stop/۴ تارگت/تاریخ/فوتر، بدون هیچ خط اضافه‌ی دیگر (مثل منبع انتخاب یا
    # عملکرد نسبی به BTC که قبلاً گاهی اضافه می‌شد و فرمت را از حالت مرجع خارج می‌کرد).
    target_lines = "\n".join(
        f"🎯 Target {lvl['n']}: {format_signal_price(_target_price(sig['price'], trade['sl'], sig['side'], lvl['r']))}"
        for lvl in TARGET_LEVELS
    )
    body = (
        f"Entry: <b>{format_signal_price(sig['price'])}</b>\n"
        f"❌ Stop: <b>{format_signal_price(trade['sl'])}</b>\n"
        f"{target_lines}"
    )

    return f"{emoji} {side_label} — {label} {tf_en}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def format_rr_exit_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                            event: Dict[str, Any], source_label: Optional[str] = None) -> str:
    level = event["level"]
    lvl_cfg = next(l for l in TARGET_LEVELS if l["n"] == level)
    side_label = "LONG" if trade["side"] == "BUY" else "SHORT"
    label = _clean_symbol_label(symbol, display_name)
    tf_en = timeframe_label
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    # طبق درخواست کاربر: عنوان و بدنه‌ی پیام دقیقاً مطابق تصویر نمونه -
    # "✅ Target N HIT (rR) — SYMBOL TF" + "Target N reached on this LONG/SHORT
    # trade." + "Entry X · Now Y" + خط قیچی + "📊 Result so far: ...R (...)"
    body = (
        f"Target {level} reached on this {side_label} trade.\n"
        f"Entry {format_signal_price(trade['entry'])}  ·  Now {format_signal_price(event['price'])}\n\n"
        f"{TARGET_ACTION_LINE.get(level, '')}\n\n"
        f"{_result_line(trade)}"
    )
    return f"✅ Target {level} HIT ({lvl_cfg['r']}R) — {label} {tf_en}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def _result_line(trade: Dict[str, Any]) -> str:
    """طبق درخواست کاربر: عبارت‌بندی دقیقاً مطابق ربات مرجع («📊 Result so far:
    <b>+2.10R</b> (profit)») - برخلاف نسخه‌ی قبلی این تابع، ربات مرجع همیشه از «Result
    so far» استفاده می‌کند (حتی در پیام‌های نهایی بسته‌شدن معامله - استاپ/رانر)، نه
    فقط «Result»، پس دیگر بر اساس closed تغییر نمی‌کند. کلمه‌ی profit/breakeven/loss
    همیشه از روی علامت واقعی final_r محاسبه می‌شود، نه از نوع بسته‌شدن، تا همیشه
    دقیق باشد."""
    result_r = compute_final_r(trade)
    word = "profit" if result_r > 0 else ("breakeven" if result_r == 0 else "loss")
    return f"📊 Result so far: <b>{result_r:+.2f}R</b> ({word})"


# ⚠️ طبق درخواست صریح کاربر («پیام رسیدن به تارگت‌ها/استاپ‌لاس و نحوه‌ی بسته‌شدن
# معامله دقیقاً مانند ربات مرجع، عیناً با همان نگارش»): این ۵ تابع جایگزین تابع
# یکپارچه‌ی قبلی (format_exit_message) شدند و متن‌شان کلمه‌به‌کلمه از توابع هم‌نامِ
# format_stop_message/format_breakeven_message/format_sl_after_t2_message/
# format_sl_after_t3_message/format_runner_stop_message در candle_engine.py
# (ربات مرجع) کپی شده - فقط جای‌گذاری symbol/display_name/timeframe_label برای
# اینکه روی هر آلت‌کوینی هم کار کند. نگاشت close_type این فایل به close_type مرجع:
#   stop → stop  |  breakeven → breakeven  |  stop_at_t1 → sl_after_t2 (بعد از
#   تارگت ۲، حد ضرر روی تارگت ۱ بود)  |  stop_at_t2 → sl_after_t3 (بعد از تارگت ۳،
#   حد ضرر روی تارگت ۲ بود)  |  runner_stop → runner_stop

def format_stop_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                         event: Dict[str, Any]) -> str:
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"Stop-loss hit on this {direction} trade before Target 1 — full position closed.\n"
        f"Entry was {format_signal_price(trade['entry'])}  ·  Stop {format_signal_price(event['price'])}"
        f"{_result_line(trade)}"
    )
    return f"❌ STOP HIT — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def format_breakeven_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                              event: Dict[str, Any]) -> str:
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"Price returned to entry on this {direction} trade — the remaining 80% closed with no "
        f"loss (20% was already banked at Target 1).\n"
        f"Entry {format_signal_price(trade['entry'])}"
        f"{_result_line(trade)}"
    )
    return f"⚪ BREAKEVEN — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def format_sl_after_t2_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                                event: Dict[str, Any]) -> str:
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"Price came back to the Target 1 level on this {direction} trade — the remaining 50% "
        f"closed there, locking in profit (20% @ Target 1, 30% @ Target 2 already banked).\n"
        f"Entry {format_signal_price(trade['entry'])}  ·  Closed at {format_signal_price(event['price'])}"
        f"{_result_line(trade)}"
    )
    return f"🔒 STOP AFTER TARGET 2 — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def format_sl_after_t3_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                                event: Dict[str, Any]) -> str:
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"Price came back to the Target 2 level on this {direction} trade — the remaining 35% "
        f"closed there, locking in more profit (20% @ T1, 30% @ T2, 15% @ T3 already banked).\n"
        f"Entry {format_signal_price(trade['entry'])}  ·  Closed at {format_signal_price(event['price'])}"
        f"{_result_line(trade)}"
    )
    return f"🔒 STOP AFTER TARGET 3 — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def format_runner_stop_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                                event: Dict[str, Any]) -> str:
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"All 4 targets were already banked on this {direction} trade (75% of the position) — "
        f"the final 25% runner portion just closed on its trailing stop (1.5R behind the peak). "
        f"Trade fully complete.\n"
        f"Entry {format_signal_price(trade['entry'])}  ·  Runner closed at {format_signal_price(event['price'])}"
        f"{_result_line(trade)}"
    )
    return f"🏁 RUNNER CLOSED — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


_EXIT_FORMATTERS = {
    "stop": format_stop_message,
    "breakeven": format_breakeven_message,
    "stop_at_t1": format_sl_after_t2_message,
    "stop_at_t2": format_sl_after_t3_message,
    "runner_stop": format_runner_stop_message,
}


def format_exit_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                         event: Dict[str, Any], source_label: Optional[str] = None) -> str:
    """پخش‌کننده - بر اساس close_type معامله، دقیقاً همان تابعِ متن-مرجع را صدا
    می‌زند (بالا). source_label فقط برای سازگاری امضای فراخوانی نگه داشته شده."""
    close_type = trade.get("close_type", "stop")
    formatter = _EXIT_FORMATTERS.get(close_type, format_stop_message)
    return formatter(symbol, display_name, timeframe_label, trade, event)


def format_forced_close_message(symbol: str, display_name: str, timeframe_label: str, trade: Dict[str, Any],
                                 exit_price: float, source_label: Optional[str] = None) -> str:
    """متن دقیقاً کپی‌شده از format_forced_close_message در candle_engine.py (ربات
    مرجع) - طبق درخواست کاربر، عیناً همان نگارش، فقط با جای‌گذاری نماد/تایم‌فریم."""
    label = _clean_symbol_label(symbol, display_name)
    direction = "LONG" if trade["side"] == "BUY" else "SHORT"
    opposite = "SHORT" if trade["side"] == "BUY" else "LONG"
    result_r = compute_final_r(trade)
    result_word = "profit" if result_r > 0 else ("breakeven" if result_r == 0 else "loss")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"This is <b>not a hedge</b> — the trend reversed, so a new {opposite} signal is "
        f"replacing this {direction} trade. Closing it now at market so nothing is left ambiguous.\n"
        f"Entry {format_signal_price(trade['entry'])}  ·  Closed at {format_signal_price(exit_price)}\n"
        f"Result: ~{result_r:+.2f}R ({result_word})\n\n"
        f"👉 A new signal for this reversal follows right after this message."
    )
    return f"⚠️ TRADE CLOSED — {label} {timeframe_label}\n\n{body}\n\n{ts}\n\n{SIGNAL_FOOTER}"


def candle_signal_scan_job(bot: "TradeiscoolBot", state: Dict[str, Any], now_utc: datetime) -> List[Dict[str, Any]]:
    """
    اسکن سیگنال کندلی برای واچ‌لیست × همه‌ی تایم‌فریم‌های فعال (فقط آن‌هایی که
    is_timeframe_due همین الان لازمشان بداند). برای هرکدام از ترکیب‌های
    نماد+تایم‌فریم یک گزارش وضعیت برمی‌گرداند (موفق/ناموفق/سیگنال/دلیل سکوت).
    """
    candle_states = state.setdefault("candle_signals", {})
    trade_history = state.setdefault("trade_history", [])
    target_events = state.setdefault("target_events", [])
    sent_count = 0
    report: List[Dict[str, Any]] = []
    processed_this_run = set()  # پرهیز از فچ/پردازش تکراری وقتی یک نماد در چند گروه (مثلاً هم ۳۰-برتر هم پرتحرک) باشد
    scan_start_ts = time.time()

    def _time_budget_exceeded() -> bool:
        """ضامن ایمنی: این اسکن نباید هیچ‌وقت آن‌قدر طول بکشد که مجبور به لغو دستی
        (Cancel) شود. اگر از این سقف گذشتیم، بقیه‌ی نمادهای این اجرا رد می‌شوند و
        اجرای بعدی (۱۵ دقیقه‌ی دیگر) ادامه می‌دهد - بدون از دست رفتن دائمی هیچ
        پنجره‌ای، چون فقط تایم‌فریم‌هایی که واقعاً کامل چک شدند mark_*_processed می‌شوند."""
        return (time.time() - scan_start_ts) > MAX_SCAN_SECONDS

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
    # تایم‌فریم، هر ۳ آلت‌کوین واچ‌لیست (SOL/XRP/ADA) پشت‌سرهم و در کوتاه‌ترین فاصله‌ی
    # ممکن از هم چک می‌شوند - به‌جای اینکه مثلاً چک ADA برای همان کندل به‌خاطر چک‌شدن
    # چند تایم‌فریم دیگر برای SOL قبل از آن، چند ده ثانیه عقب بیفتد.
    # ⚠️ "1m" عمداً از این حلقه (واچ‌لیست اصلی SOL/XRP/ADA) حذف می‌شود - این تایم‌فریم
    # فقط برای ارزهای همسو با روند و فقط از مسیر WebSocket (check_trend_alts_1m_signals
    # در حلقه‌ی اصلی main()) پردازش می‌شود، نه اینجا با REST.
    tf_order = sorted([k for k in TIMEFRAMES.keys() if k != "1m"], key=lambda k: TIMEFRAMES[k]["bar_seconds"])
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

    budget_exceeded = False
    for tf in due_timeframes:
        if budget_exceeded:
            break
        tf_cfg = TIMEFRAMES[tf]
        tf_fully_completed = True
        for symbol, display_name in WATCHLIST_SYMBOLS.items():
            if _time_budget_exceeded():
                logger.warning(f"⏱️ سقف زمانی اسکن ({MAX_SCAN_SECONDS} ثانیه) رسید - بقیه‌ی این اجرا رد می‌شود، "
                                f"اجرای بعدی (۱۵ دقیقه‌ی دیگر) ادامه می‌دهد")
                budget_exceeded = True
                tf_fully_completed = False
                break
            state_key = f"{symbol}|{tf}"
            entry = {"symbol": symbol, "timeframe": tf, "display_name": display_name, "ok": False,
                      "signal": None, "error": None, "note": None}
            fetch_fn = lambda sym, lim, _iv=tf_cfg["td_interval"], _bs=tf_cfg["bar_seconds"]: \
                fetch_closed_klines_twelvedata(sym, lim, _iv, _bs)
            try:
                sym_state = candle_states.get(state_key)
                new_state, sent, err = process_and_send_symbol_tf(bot, fetch_fn, symbol, display_name, tf, tf_cfg, sym_state, trade_history=trade_history, target_events=target_events, global_state=state)
                if new_state is None:
                    entry["error"] = err or "دریافت داده از Twelve Data ناموفق بود (ممکن است این نماد/تایم‌فریم نیاز به پلن پولی داشته باشد)"
                else:
                    candle_states[state_key] = new_state
                    entry["ok"] = True
                    entry["note"] = _describe_silence(new_state)
                    processed_this_run.add(state_key)
                    if sent:
                        sent_count += sent
                        entry["signal"] = f"{sent} پیام"
                time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data (۸ درخواست در دقیقه در پلن رایگان)
            except Exception as e:
                entry["error"] = str(e)
                logger.warning(f"⚠️ خطا در پردازش {symbol} [{tf}]: {e}")
            report.append(entry)
        # این تایم‌فریم فقط اگر برای همه‌ی نمادهای واچ‌لیست کامل چک شد، پنجره ثبت
        # می‌شود - وگرنه (قطع به‌خاطر سقف زمانی) اجرای بعدی همین پنجره را کامل می‌کند
        if tf_fully_completed:
            mark_timeframe_processed(tf, now_utc, state)

    # ---------- گروه دوم: ۳۰ ارز برتر بازار بر اساس مارکت‌کپ (فقط ۴ ساعته) ----------
    if budget_exceeded:
        pass
    elif TOP30_ENABLED and is_top30_due(now_utc, state):
        if not TWELVEDATA_API_KEY:
            logger.warning("⚠️ TWELVEDATA_API_KEY تنظیم نشده - اسکن ۳۰ ارز برتر رد شد")
        else:
            top_coins = bot.get_top_coins_from_cmc(TOP30_COUNT + 5)  # چند تای اضافه برای اطمینان بعد از فیلتر
            # BTC/ETH چون این کانال کاملاً اختصاصی آلت‌کوین است حذف می‌شوند؛ نمادهای
            # واچ‌لیست اصلی (SOL/XRP/ADA) هم حذف می‌شوند چون همان‌ها از قبل با تایم‌فریم
            # کامل (۵د+۱۵د+۱ساعته) در گروه اول پردازش شدند - پردازش دوباره‌شان اینجا
            # (فقط ۱ساعته) هم سهمیه‌ی API تلف می‌کرد هم می‌توانست دو سیگنال جدا روی یک
            # نماد در یک تایم‌فریم مشترک باز کند.
            excluded = {"BTC", "ETH"} | set(WATCHLIST_SYMBOLS.values())
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
                top30_had_error = False
                for cmc_symbol in top30_symbols:
                    if _time_budget_exceeded():
                        logger.warning(f"⏱️ سقف زمانی اسکن رسید - بقیه‌ی ۳۰ ارز برتر رد شد، پنجره برای اجرای بعدی نگه داشته می‌شود")
                        budget_exceeded = True
                        break
                    td_symbol = f"{cmc_symbol}/USD"
                    state_key = f"{td_symbol}|{TOP30_TIMEFRAME}"
                    if state_key in processed_this_run:
                        continue  # همین نماد+تایم‌فریم قبلاً در همین اجرا پردازش شده
                    entry = {"symbol": td_symbol, "timeframe": TOP30_TIMEFRAME, "display_name": cmc_symbol, "ok": False,
                              "signal": None, "error": None, "note": None}
                    try:
                        sym_state = candle_states.get(state_key)
                        new_state, sent, err = process_and_send_symbol_tf(
                            bot, fetch_fn, td_symbol, cmc_symbol, TOP30_TIMEFRAME, tf_cfg, sym_state,
                            trade_history=trade_history, source_label="۳۰ ارز برتر بازار", target_events=target_events,
                            global_state=state)
                        if new_state is None:
                            entry["error"] = err or "دریافت داده از Twelve Data ناموفق بود (ممکن است این ارز پشتیبانی نشود)"
                        else:
                            candle_states[state_key] = new_state
                            entry["ok"] = True
                            entry["note"] = _describe_silence(new_state)
                            processed_this_run.add(state_key)
                            if sent:
                                sent_count += sent
                                entry["signal"] = f"{sent} پیام"
                        time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data
                    except Exception as e:
                        entry["error"] = str(e)
                        top30_had_error = True
                        logger.warning(f"⚠️ خطا در پردازش {td_symbol}: {e}")
                    report.append(entry)
                # ⚠️ اگر حداقل یک نماد خطا داد، پنجره «کامل پردازش‌شده» علامت زده
                # نمی‌شود - وگرنه تا نوبت بعدی روز (ساعت‌ها بعد) دوباره تلاش نمی‌شد.
                # الان دور بعدی (۵ دقیقه‌ی دیگر) همه‌ی ۳۰ ارز را دوباره چک می‌کند -
                # نمادهایی که قبلاً موفق پردازش شدند از طریق processed_this_run رد
                # می‌شوند (بدون هزینه‌ی اضافه)، فقط نماد(های) خطادار دوباره امتحان می‌شوند.
                if not budget_exceeded and not top30_had_error:
                    mark_top30_processed(now_utc, state)

    # ---------- گروه سوم: ارزهای همسو با روند بازار (نسبت به BTC، انتخاب روزانه) ----------
    # طبق درخواست کاربر: اول جهت کلی بازار تشخیص داده شده (refresh_trend_alts، صعودی/
    # نزولی/خنثی)، بعد فقط همان طرف (برنده اگر صعودی، بازنده اگر نزولی) انتخاب شده -
    # اینجا فقط ۵د/۱۵د/۱ساعته چک می‌شود؛ تایم‌فریم ۱د جدا و هر چند ثانیه در حلقه‌ی
    # اصلی از WebSocket چک می‌شود (check_trend_alts_1m_signals)، نه اینجا.
    regime = state.get("trend_alts", {}).get("regime")
    trend_alts = get_trend_alts_to_process(state, candle_states)
    if trend_alts and not budget_exceeded:
        for tf in TREND_ALTS_TIMEFRAMES:
            if budget_exceeded:
                break
            if not is_trend_alts_tf_due(tf, now_utc, state):
                continue
            tf_cfg = TIMEFRAMES[tf]
            logger.info(f"📈 در حال بررسی {len(trend_alts)} ارز همسو با روند بازار ({regime}, تایم‌فریم {tf_cfg['label']}) از Twelve Data...")
            fetch_fn = lambda sym, lim, _iv=tf_cfg["td_interval"], _bs=tf_cfg["bar_seconds"]: \
                fetch_closed_klines_twelvedata(sym, lim, _iv, _bs)
            tf_fully_completed = True
            trend_alts_had_error = False
            for mover in trend_alts:
                if _time_budget_exceeded():
                    logger.warning(f"⏱️ سقف زمانی اسکن رسید - بقیه‌ی ارزهای همسو با روند رد شد، پنجره برای اجرای بعدی نگه داشته می‌شود")
                    budget_exceeded = True
                    tf_fully_completed = False
                    break
                cmc_symbol = mover["symbol"]
                td_symbol = f"{cmc_symbol}/USD"
                state_key = f"{td_symbol}|{tf}"
                if state_key in processed_this_run:
                    continue
                entry = {"symbol": td_symbol, "timeframe": tf, "display_name": cmc_symbol, "ok": False,
                          "signal": None, "error": None, "note": None}
                try:
                    sym_state = candle_states.get(state_key)
                    new_state, sent, err = process_and_send_symbol_tf(
                        bot, fetch_fn, td_symbol, cmc_symbol, tf, tf_cfg, sym_state,
                        trade_history=trade_history, source_label=f"ارز همسو با روند ({mover['reason']})",
                        target_events=target_events, global_state=state)
                    if new_state is None:
                        entry["error"] = err or "دریافت داده از Twelve Data ناموفق بود (ممکن است این ارز پشتیبانی نشود)"
                    else:
                        candle_states[state_key] = new_state
                        entry["ok"] = True
                        entry["note"] = _describe_silence(new_state)
                        processed_this_run.add(state_key)
                        if sent:
                            sent_count += sent
                            entry["signal"] = f"{sent} پیام"
                    time.sleep(7.5)  # رعایت محدودیت نرخ درخواست Twelve Data
                except Exception as e:
                    entry["error"] = str(e)
                    trend_alts_had_error = True
                    logger.warning(f"⚠️ خطا در پردازش {td_symbol} [{tf}] (همسو با روند): {e}")
                report.append(entry)
            # ⚠️ همان رفع باگ ۳۰-ارز-برتر: اگر حداقل یک نماد خطا داد، این پنجره
            # «کامل پردازش‌شده» علامت زده نمی‌شود تا دور بعدی (۵ دقیقه‌ی دیگر) دوباره
            # تلاش شود، نه فقط در نوبت بعدی (که می‌تواند ساعت‌ها بعد باشد).
            if tf_fully_completed and not trend_alts_had_error:
                mark_trend_alts_tf_processed(tf, now_utc, state)

    logger.info("——— گزارش تشخیصی اسکن کندلی ———")
    for e in report:
        tf_label = TIMEFRAMES.get(e["timeframe"], {}).get("label", e["timeframe"])
        if e["signal"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ✅ {e['signal']} ارسال شد")
        elif e["error"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ❌ خطا - {e['error']}")
        elif e["ok"]:
            logger.info(f"  {e['symbol']} [{tf_label}] ({e['display_name']}): ⚪ بدون سیگنال جدید - {e['note']}")
    logger.info(f"✅ اسکن کندلی کامل شد. {sent_count} پیام ارسال شد.")
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

    def send_telegram_photo(self, caption: str, photo_path: Optional[str] = None,
                             reply_to_message_id: Optional[int] = None, chat_id: str = None) -> Optional[int]:
        """پیام (عکس+کپشن، یا فقط متن اگه چارت ساخته نشد) رو می‌فرسته و در صورت موفقیت
        message_id تلگرام رو برمی‌گردونه (برای اینکه پیام‌های تارگت/استاپ بعدی بتونن
        روی پیام سیگنال ورود اصلی ریپلای بزنن). در صورت شکست None برمی‌گردونه."""
        if not TELEGRAM_BOT_TOKEN:
            logger.error("تنظیمات تلگرام وجود ندارد")
            return None
        if not chat_id:
            chat_id = TELEGRAM_CHANNEL_ID
        try:
            data = {"chat_id": chat_id, "parse_mode": "HTML"}
            if reply_to_message_id:
                data["reply_to_message_id"] = reply_to_message_id
                data["allow_sending_without_reply"] = True
            if photo_path and os.path.exists(photo_path):
                data["caption"] = caption
                with open(photo_path, "rb") as f:
                    resp = self.session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                        data=data, files={"photo": f}, timeout=60,
                    )
            else:
                data["text"] = caption
                data["disable_web_page_preview"] = True
                resp = self.session.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    data=data, timeout=HTTP_TIMEOUT,
                )
            result = resp.json()
            ok = resp.status_code == 200 and result.get("ok")
            if not ok:
                logger.error(f"❌ خطا در ارسال عکس/پیام تلگرام: {resp.text}")
                return None
            return result["result"]["message_id"]
        except Exception as e:
            logger.error(f"❌ خطا در ارسال عکس/پیام به تلگرام: {e}")
            return None

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

    # ---------------- ارزهای همسو با روند بازار (نسبت به BTC، انتخاب روزانه) ----------------
    def get_trend_alts(self, count: int = TREND_ALTS_CANDIDATE_POOL_SIZE) -> Optional[Dict[str, Any]]:
        """
        اول جهت کلی بازار را از روی BTC تشخیص می‌دهد (get_market_regime)، سپس فقط
        همان یک طرف را برمی‌گرداند: اگر صعودی → بیشترین صعود نسبت به BTC، اگر نزولی
        → بیشترین نزول نسبت به BTC. اگر بازار خنثی بود، لیست خالی برمی‌گرداند (آن
        روز این گروه سیگنالی صادر نمی‌کند - بهتر از انتخاب بی‌معنا در بازار بلاتکلیف).
        🆕 برای پشتیبانی از جایگزینی خودکار بعد از استاپ‌های پیاپی (get_active_trend_alts)،
        الان به‌جای فقط TREND_ALTS_COUNT، یک استخر رتبه‌بندی‌شده‌ی بزرگ‌تر (count، پیش‌فرض
        TREND_ALTS_CANDIDATE_POOL_SIZE) برمی‌گرداند.
        """
        try:
            top_coins = self.get_top_coins_from_cmc(150)
            if not top_coins:
                return None
            regime, btc_change = get_market_regime(top_coins)
            if regime == "neutral":
                return {"regime": regime, "btc_change": btc_change, "symbols": []}

            performance_data = []
            for coin in top_coins:
                try:
                    symbol = coin['symbol']
                    if symbol in STABLECOINS or symbol in ("BTC", "ETH") or symbol in WATCHLIST_SYMBOLS.values():
                        continue  # کانال کاملاً اختصاصی آلت‌کوین است - BTC خودِ معیار مقایسه است، ETH هم طبق درخواست
                                  # حذف شد؛ نمادهای واچ‌لیست اصلی هم حذف می‌شوند چون از قبل با تایم‌فریم کامل پوشش دارند
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

            if regime == "bull":
                chosen = sorted(performance_data, key=lambda x: x['relative_performance'], reverse=True)[:count]
                reason = "برنده نسبت به BTC (بازار صعودی)"
            else:
                chosen = sorted(performance_data, key=lambda x: x['relative_performance'])[:count]
                reason = "بازنده نسبت به BTC (بازار نزولی)"

            return {"regime": regime, "btc_change": btc_change,
                     "symbols": [{**c, 'reason': reason} for c in chosen]}
        except Exception as e:
            logger.error(f"❌ خطا در دریافت لیست ارزهای همسو با روند بازار: {e}")
            return None

    def refresh_trend_alts(self, state: Dict[str, Any]) -> None:
        result = self.get_trend_alts(TREND_ALTS_CANDIDATE_POOL_SIZE)
        if result is None:
            logger.warning("⚠️ دریافت لیست ارزهای همسو با روند بازار ناموفق بود - لیست قبلی (در صورت وجود) نگه داشته می‌شود")
            return
        state['trend_alts'] = {
            'regime': result['regime'],
            'btc_change': result['btc_change'],
            'candidates': result['symbols'],   # استخر کامل رتبه‌بندی‌شده - get_active_trend_alts اولین ۳ تای غیرقفل را انتخاب می‌کند
            'updated': datetime.now(timezone.utc).isoformat(),
        }
        if result['regime'] == 'neutral':
            logger.info(f"📊 بازار خنثی است (BTC 24h: {format_percent(result['btc_change'])}) - "
                        f"امروز گروه «ارزهای همسو با روند» غیرفعال می‌ماند")
        else:
            direction_fa = "صعودی" if result['regime'] == 'bull' else "نزولی"
            active_preview = get_active_trend_alts(state)
            logger.info(f"📈 بازار {direction_fa} است (BTC 24h: {format_percent(result['btc_change'])}) - "
                        f"لیست فعال امروز: {', '.join(c['symbol'] for c in active_preview)} "
                        f"(استخر کامل: {', '.join(c['symbol'] for c in result['symbols'])})")

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
            # نکته‌ی مهم درباره‌ی هماهنگی با ربات دوم (subscription_bot.py + candle_engine.py):
            # ایموجی سیگنال‌های کندلی واقعی (Entry/Stop دارند) عمداً 🟢/🔴 نگه داشته شده تا
            # فوروارد ادمین به کانال دوم به‌صورت خودکار تشخیص داده شود (starts_with_signal_emoji
            # در subscription_bot.py دقیقاً همین دو ایموجی را چک می‌کند). سیگنال جریان استیبل‌کوین
            # (FLOW) هرگز Entry:/Stop: ندارد (طبق طراحی - جریان صرافی SL/تارگت طبیعی ندارد)، پس اگر
            # همان 🟢/🔴 را داشته باشد، فوروارد آن باعث می‌شود ربات دوم آن را «سیگنال ناقص» تشخیص
            # دهد و یک پیام هشدار «Entry/Stop پیدا نشد» در کانال خصوصی برگرداند - نویز بی‌مورد.
            # با 🔵 (لانگ) و 🟠 (شورت) این پیام‌ها دیگر با SIGNAL_EMOJI_PREFIX تداخل ندارند و اگر
            # ادمین به اشتباه فورواردشان کند، ربات دوم بی‌سروصدا نادیده‌شان می‌گیرد.
            emoji = "🔵" if signal_data['side'] == "LONG" else "🟠"
            label = signal_data['symbol'].upper()
            body = (
                f"Score: {signal_data['total_score']}/12\n"
                f"Price: {format_signal_price(signal_data['price'])} | Market cap: {format_number(signal_data['market_cap'])}\n"
                f"Net stablecoin flow: {format_number(signal_data['net_flow'])} "
                f"(in {format_number(signal_data['inflow'])} / out {format_number(signal_data['outflow'])})\n"
                f"Relative performance vs BTC: {format_percent(signal_data['relative_performance'])}"
            )
            return f"{emoji} {signal_data['side']} — {label} (Stablecoin Flow)\n\n{body}\n\n{signal_data['timestamp']}\n\n{SIGNAL_FOOTER}"
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

            # 🆕 شمارش روزانه‌ی سیگنال‌های FLOW (طبق درخواست کاربر: «تمام سیگنال‌های صادرشده»
            # باید در گزارش روزانه شناسایی شوند). این سیگنال‌ها ذاتاً Entry/Stop ندارند (جریان
            # صرافی، نه یک معامله‌ی R-based) پس نمی‌توانند مثل بقیه در trade_history/برد-باخت
            # حساب شوند - ولی حداقل تعدادشان اینجا (به تفکیک روز UTC) ثبت می‌شود تا گزارش
            # روزانه بگوید «امروز این تعداد هم پست شد»، نه این‌که کاملاً نادیده گرفته شوند.
            if sent_count:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                counters = self._state_ref.setdefault("flow_signals_today", {})
                if counters.get("date") != today_str:
                    counters["date"] = today_str
                    counters["count"] = 0
                counters["count"] = counters.get("count", 0) + sent_count

            logger.info(f"✅ اسکن جریان استیبل‌کوین کامل شد. {sent_count} سیگنال ارسال شد")
        except Exception as e:
            logger.error(f"❌ خطا در اسکن جریان: {e}")


# ==================================================================
# اجرای اصلی - یک بار اجرا می‌شود و خارج می‌شود (سازگار با GitHub Actions)
# ==================================================================
# توجه: run_live_gold_check_pass (رهگیری REST قیمت زنده‌ی معاملات باز طلا) طبق
# درخواست کاربر حذف شد، چون طلا کامل از WATCHLIST_SYMBOLS حذف شده و دیگر هیچ
# معامله‌ی باز XAU/USD ای وجود نخواهد داشت.


def run_scan_cycle(state: Dict[str, Any]) -> None:
    """یک دور کامل اسکن: سیگنال جدید در همه‌ی گروه‌ها (واچ‌لیست/۳۰-برتر/پرتحرک) +
    اسکن جریان استیبل‌کوین + گزارش روزانه (هر کدام فقط اگر due باشند). این تابع هر
    SCAN_CYCLE_INTERVAL_SECONDS از حلقه‌ی اصلی صدا زده می‌شود."""
    now_utc = datetime.now(timezone.utc)
    load_td_unsupported_cache(state)
    logger.info(f"🕐 دور اسکن - ساعت فعلی UTC: {now_utc.strftime('%Y-%m-%d %H:%M')}")

    run_flow_perf = FORCE_RUN_ALL or is_flow_perf_due(now_utc, state)
    bot = TradeiscoolBot(state)

    if run_flow_perf:
        if COINMARKETCAP_API_KEY and LIVECOINWATCH_API_KEY:
            bot.flow_and_performance_scan_job()
        else:
            logger.warning("⚠️ COINMARKETCAP_API_KEY یا LIVECOINWATCH_API_KEY تنظیم نشده - اسکن جریان رد شد")
        mark_flow_perf_processed(now_utc, state)

    # 🆕 بروزرسانی روزانه‌ی «ارزهای همسو با روند بازار» - جدا از چرخه‌ی ۳ساعته‌ی بالا،
    # چون این گروه فقط یک‌بار در روز (TREND_ALTS_REFRESH_HOUR) باید تازه شود. اگر
    # ابتدای همین اجرا (main()) از قبل امروز را تازه کرده باشد، is_trend_alts_refresh_due
    # اینجا False برمی‌گردد و این بلوک بدون هیچ هزینه‌ای رد می‌شود؛ این فراخوانی فقط
    # برای اجراهایی لازم است که از نیمه‌شب UTC عبور می‌کنند (نادر، طول هر اجرا
    # ~۵ساعت و ۲۰دقیقه است) - در آن حالت لیست جدید در همین اجرا برای تایم‌فریم‌های
    # REST (۵د/۱۵د/۱ساعته) بلافاصله اعمال می‌شود؛ اتصال WebSocket/کندل ۱د تا اجرای
    # بعدی همان لیست قدیمی را نگه می‌دارد (توضیح کامل در start_price_stream).
    if TREND_ALTS_ENABLED and is_trend_alts_refresh_due(now_utc, state):
        if COINMARKETCAP_API_KEY:
            # 🔴 رفعِ باگِ «پیام تارگت/نتیجه گاهی تکراری ارسال می‌شود»: قبلاً این بلوک
            # try/except نداشت. اگر bot.refresh_trend_alts (یک درخواست شبکه‌ی معمولی به
            # CoinMarketCap) خطا می‌داد، آن خطا از اینجا مستقیم بالا می‌رفت و کل
            # run_scan_cycle را متوقف می‌کرد - که در حلقه‌ی اصلی (main) باعث می‌شد کل
            # state این دور (شامل هر Target HIT/Stop/Breakeven‌ای که خودِ WebSocket زنده
            # - check_ws_open_trades - همین چند دقیقه‌ی قبل از این دور، از قبل با موفقیت
            # به کانال فرستاده و trade["hit"][...] را True کرده بود) به نسخه‌ی backupِ
            # *قبل از* آن تشخیص برگردد. یعنی چند ثانیه بعد، همان دور بعدیِ
            # check_ws_open_trades دوباره قیمت زنده را با trade["hit"][...] اکنون
            # (اشتباهاً) False می‌دید و همان پیام «Target N HIT» را که دقیقه‌ها قبل واقعاً
            # در کانال ارسال شده بود، دوباره می‌فرستاد - دقیقاً همان الگوی باگِ گزارش
            # روزانه‌ی تکراری، این‌بار روی پیام‌های آپدیت سیگنال. الان یک خطای اینجا فقط
            # لاگ می‌شود و بقیه‌ی دور اسکن (candle_signal_scan_job و گزارش‌ها) طبیعی ادامه
            # پیدا می‌کند - این بروزرسانی فقط یک‌بار در روز لازم است، اجرای بعدی
            # (حداکثر ۱۵ دقیقه‌ی دیگر) دوباره امتحان می‌کند.
            try:
                bot.refresh_trend_alts(state)
                mark_trend_alts_refresh_processed(now_utc, state)
            except Exception as e:
                logger.error(f"❌ بروزرسانی ارزهای همسو با روند بازار ناموفق بود - دور اسکن ادامه می‌دهد: {e}")
        else:
            logger.warning("⚠️ COINMARKETCAP_API_KEY تنظیم نشده - بروزرسانی ارزهای همسو با روند رد شد")

    # 🔴 همان دلیل بالا: اگر خودِ اسکن سیگنال کندلی (که برای هر نماد/تایم‌فریم داخلی
    # خودش قبلاً try/except جدا دارد، ولی کد راه‌اندازی/دروازه‌ی اطراف آن - مثل فچ لیست
    # ۳۰ ارز برتر - ندارد) با یک خطای پیش‌بینی‌نشده کامل متوقف شود، این try/except اجازه
    # نمی‌دهد آن خطا کل run_scan_cycle (و در نتیجه کل state این دور، شامل هر سیگنال
    # زنده‌ای که همین الان از WebSocket پردازش و ارسال شده) را قربانی کند.
    try:
        candle_signal_scan_job(bot, state, now_utc)
    except Exception as e:
        logger.error(f"❌ اسکن سیگنال کندلی با خطای پیش‌بینی‌نشده متوقف شد - دور اسکن ادامه می‌دهد: {e}")

    # ⚠️ الگوی «pull → mark → push → send» (رفع باگ تکرار گزارش روزانه، عیناً مثل
    # candle_engine.py): قبل از هر تصمیمی یک pull تازه می‌کنیم تا اگر اجرای دیگری
    # (مثلاً یک instance موازی که به‌خاطر صف‌شدن concurrency هم‌زمان فعال است) همین
    # الان گزارش را زده و پوش کرده، حتماً ببینیمش؛ و اگر due بود، بلافاصله (قبل از
    # ساختن/فرستادن خودِ پیام) پرچم را mark+save+push می‌کنیم - نه بعدش - تا اگر
    # ارسال پیام طول بکشد یا پروسه وسط کار قطع شود، اجرای دیگری همین گزارش را
    # دوباره due نبیند.
    git_commit_and_push()
    _refresh_tf_windows_from_disk(state)

    # 🔴 همان الگوی بالا: این بلوک هم قبلاً try/except نداشت. یک خطای پیش‌بینی‌نشده در
    # ساخت/ارسال گزارش روزانه (مثلاً یک رکورد ناقص در trade_history) بعد از این‌که
    # candle_signal_scan_job چند خط بالاتر با موفقیت چند پیام Target HIT/Stop واقعی
    # فرستاده، دقیقاً همان چرخه‌ی معیوب بالا را دوباره می‌ساخت - این‌بار قربانی‌شونده
    # سیگنال‌های خودِ همین دور، نه فقط دورهای WebSocket قبلی.
    try:
        if is_results_report_due(now_utc, state):
            mark_results_report_processed(now_utc, state)
            save_state(state)
            git_commit_and_push()

            trade_history = state.get("trade_history", [])
            target_events = state.get("target_events", [])
            flow_counters = state.get("flow_signals_today", {})
            flow_today = flow_counters.get("count", 0) if flow_counters.get("date") == now_utc.strftime("%Y-%m-%d") else 0
            report_msg = format_results_message(trade_history, now_utc, target_events=target_events,
                                                 flow_signals_today=flow_today)
            if report_msg:
                if bot.send_telegram_message(report_msg):
                    logger.info("📤 گزارش روزانه‌ی عملکرد سیگنال‌ها ارسال شد")
                else:
                    logger.error("📤 ارسال گزارش روزانه ناموفق بود - امروز دوباره تلاش نمی‌شود "
                                  "(برای جلوگیری از ارسال دوباره)؛ لاگ‌ها را چک کنید.")
            else:
                logger.info("ℹ️ هنوز هیچ معامله‌ای بسته نشده - گزارش روزانه رد شد")
    except Exception as e:
        logger.error(f"❌ ساخت/ارسال گزارش روزانه با خطا مواجه شد - دور اسکن ادامه می‌دهد: {e}")

    bot.sync_state()
    # 🔴 لایه‌ی دوم دفاعی (لایه‌ی اول: عدم‌عقب‌گردِ _refresh_tf_windows_from_disk
    # بالاتر، لایه‌ی سوم: همان تابع در مسیر revert کردن state در حلقه‌ی اصلی main) در
    # برابر همان چرخه‌ی معیوب «گزارش روزانه ده‌ها بار تکرار می‌شود»: این
    # کش صرفاً یک بهینه‌سازی (رد سریع‌تر نمادهای شناخته‌شده‌ی بدون‌پشتیبانی) است -
    # هیچ داده‌ی حیاتی‌ای اینجا نیست. اگر نوشتنش هر بار به هر دلیلی (مثلاً یک مقدار
    # غیرقابل serialize که به‌اشتباه در دیکشنری راه پیدا کرده) خطا بدهد، دیگر اجازه
    # نمی‌دهیم آن خطا کل تابع را متوقف و در نتیجه (طبق منطق حلقه‌ی اصلی) کل state -
    # شامل پرچم «گزارش امروز فرستاده شد» که چند خط بالاتر با موفقیت ارسال و پوش شد -
    # را به عقب برگرداند.
    try:
        save_td_unsupported_cache(state)
    except Exception as e:
        logger.error(f"❌ ذخیره‌ی کش نمادهای بدون‌پشتیبانی ناموفق بود (بی‌اهمیت - نادیده گرفته می‌شود): {e}")
    logger.info("✅ دور اسکن کامل شد")


def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_BOT_TOKEN یا TELEGRAM_CHANNEL_ID تنظیم نشده - خروج")
        return
    if not TWELVEDATA_API_KEY:
        logger.error("❌ TWELVEDATA_API_KEY تنظیم نشده - خروج")
        return

    # 🔴 (تضمین «بدون قطعی») این خط state رو خارج از try تعریف می‌کنیم تا اگر بدنه‌ی
    # try خیلی زود (حتی قبل از load_state موفق) با خطا متوقف شد، بلوک finally هنوز
    # بتواند save_state/git_commit_and_push را (روی یک state خالی/جزئی، بی‌خطر) صدا
    # بزند بدون NameError، و notify_admin هم بتواند کار کند.
    state: Dict[str, Any] = {}
    stop_event = threading.Event()

    # 🔴 (تضمین «بدون قطعی» - رفع اصلی): قبلاً فقط خودِ حلقه‌ی `while` داخل try/except
    # جامع بود؛ اگر خطای پیش‌بینی‌نشده‌ای *قبل* از رسیدن به حلقه اتفاق می‌افتاد (مثلاً
    # در load_state، در بروزرسانی اولیه‌ی ارزهای همسو با روند، یا هنگام start-کردن ترد
    # WebSocket)، آن خطا کاملاً بدون محافظت بود: کل main() با یک traceback خام crash
    # می‌کرد، exit code غیرصفر برمی‌گشت، state ذخیره/پوش نمی‌شد، و از همه مهم‌تر - چون
    # این crash معمولاً در چند ثانیه‌ی اول اتفاق می‌افتد (نه بعد از ۵ ساعت و ۲۰ دقیقه‌ی
    # کامل)، اجرای بعدی که باید جایگزینش شود تا ~۵ ساعت دیگر (فاصله‌ی cron قبلی) شروع
    # نمی‌شد - یعنی ساعت‌ها قطعی واقعی. الان کل بدنه (از load_state تا انتهای حلقه)
    # زیر همین یک try/except/finally است، دقیقاً مثل رفتار حلقه‌ی اصلی.
    try:
        state = load_state()

        # 🆕 قبل از شروع ترد WebSocket، اگر امروز هنوز لیست «ارزهای همسو با روند بازار»
        # تازه نشده، همین‌جا (همگام، یک‌بار) تازه می‌شود - تا خودِ استریم زنده از همان
        # اول به نمادهای درست subscribe کند (توضیح کامل در تعریف TREND_ALTS_WS_SYMBOL_MAP
        # و start_price_stream). اگر امروز قبلاً تازه شده (مثلاً همین اجرا ادامه‌ی یک
        # کامیت state از چند ساعت پیش است)، این فقط از روی state موجود نگاشت را می‌سازد
        # و هیچ درخواست جدیدی به CoinMarketCap نمی‌زند.
        now_utc0 = datetime.now(timezone.utc)
        if TREND_ALTS_ENABLED and COINMARKETCAP_API_KEY and is_trend_alts_refresh_due(now_utc0, state):
            try:
                bot0 = TradeiscoolBot(state)
                bot0.refresh_trend_alts(state)
                mark_trend_alts_refresh_processed(now_utc0, state)
                bot0.sync_state()
                save_state(state)
            except Exception as e:
                logger.error(f"❌ بروزرسانی اولیه‌ی ارزهای همسو با روند بازار ناموفق بود: {e}")
        TREND_ALTS_WS_SYMBOL_MAP.clear()
        TREND_ALTS_WS_SYMBOL_MAP.update(build_trend_alts_ws_map(state))
        WS_SYMBOL_MAP_REVERSE.update({v: k for k, v in TREND_ALTS_WS_SYMBOL_MAP.items()})
        if TREND_ALTS_WS_SYMBOL_MAP:
            logger.info(f"📈 ارزهای همسو با روند بازار برای این اجرا: {list(TREND_ALTS_WS_SYMBOL_MAP.keys())}")

        latest_prices: Dict[str, float] = {}
        price_lock = threading.Lock()

        ws_thread = threading.Thread(target=start_price_stream, args=(latest_prices, price_lock, stop_event), daemon=True)
        ws_thread.start()
        logger.info(f"🚀 پروسه‌ی پیوسته شروع شد - WebSocket واچ‌لیست آلت‌کوین را لحظه‌ای رصد می‌کند، "
                    f"هر {SCAN_CYCLE_INTERVAL_SECONDS} ثانیه یک دور اسکن کامل، حداکثر تا "
                    f"{LOOP_MAX_SECONDS / 3600:.1f} ساعت اجرا می‌ماند")

        start = time.time()
        last_scan = 0        # صفر یعنی همین اول یک دور اسکن کامل انجام شود
        last_commit = start
        last_live_check = 0  # صفر یعنی همین اول یک دور رصد زنده‌ی REST انجام شود

        # نکته درباره‌ی همزمانی (طبق بازبینی برنامه‌نویس، بخش «مدیریت همزمانی state»):
        # این دیکشنری state فقط و فقط در همین حلقه/همین ترد اصلی خوانده/نوشته می‌شود.
        # ترد جداگانه‌ی WebSocket (ws_thread/start_price_stream) فقط به latest_prices
        # می‌نویسد (که خودش با price_lock محافظت می‌شود) و هرگز مستقیم به state دست
        # نمی‌زند - پس در طراحی فعلی، threading.Lock جداگانه برای state هیچ Race
        # Condition واقعی‌ای را رفع نمی‌کند (چیزی برای رفع‌کردن وجود ندارد) و صرفاً
        # پیچیدگی بدون فایده اضافه می‌کند؛ عمداً اضافه نشد. اگر در آینده پردازش state
        # به ترد دیگری هم منتقل شود، این نکته باید بازبینی شود.
        while time.time() - start < LOOP_MAX_SECONDS:
            try:
                bot_for_ws = TradeiscoolBot(state)
                check_ws_open_trades(bot_for_ws, state, latest_prices, price_lock)
                # 🆕 سیگنال تایم‌فریم ۱ دقیقه‌ای ارزهای همسو با روند - از کندل‌های
                # ساخته‌شده در حافظه از تیک‌های WebSocket (بدون هیچ هزینه‌ی API)،
                # هر ۵ ثانیه (WS_CHECK_INTERVAL_SECONDS) چک می‌شود.
                check_trend_alts_1m_signals(bot_for_ws, state)
                bot_for_ws.sync_state()
            except Exception as e:
                logger.error(f"❌ check_ws_open_trades خطا داد: {e}")

            if time.time() - last_live_check >= LIVE_CHECK_INTERVAL_SECONDS:
                try:
                    bot_for_live = TradeiscoolBot(state)
                    check_rest_live_trades(bot_for_live, state)
                    bot_for_live.sync_state()
                except Exception as e:
                    logger.error(f"❌ check_rest_live_trades خطا داد: {e}")
                last_live_check = time.time()

            if time.time() - last_scan >= SCAN_CYCLE_INTERVAL_SECONDS:
                # طبق بازبینی برنامه‌نویس («مدیریت خطای حلقه‌ی اصلی»): قبل از هر دور
                # اسکن یک نسخه‌ی سبک (deepcopy) از state گرفته می‌شود. اگر run_scan_cycle
                # وسط کار (مثلاً بعد از نیمه‌پردازش‌کردن چند نماد) با خطای پیش‌بینی‌نشده
                # متوقف شود، state به همین نسخه‌ی سالمِ قبل از شروع دور برمی‌گردد - به‌جای
                # اینکه یک state نیمه‌بروزشده (که می‌تواند مثلاً پنجره‌ای را «پردازش‌شده»
                # علامت بزند بدون اینکه سیگنالش واقعاً ارسال شده باشد) ذخیره و پوش شود.
                state_backup = copy.deepcopy(state)
                try:
                    run_scan_cycle(state)
                except Exception as e:
                    logger.error(f"❌ run_scan_cycle خطا داد - state به آخرین نسخه‌ی سالم قبل از "
                                 f"این دور برگردانده شد: {e}")
                    notify_admin(
                        f"یک دور اسکن (run_scan_cycle) در bot.py با خطا مواجه شد و نادیده گرفته "
                        f"شد:\n{e}\n\nحلقه‌ی اصلی همچنان ادامه دارد. اگر این پیام مکرر می‌بینید "
                        f"(علی‌رغم کول‌دان ۲۰ دقیقه‌ای)، احتمالاً یک منبع داده یا API به‌طور مداوم "
                        f"از کار افتاده.",
                        state, kind="scan_cycle_error",
                    )
                    state.clear()
                    state.update(state_backup)
                    # 🔴 حیاتی (رفع باگ «گزارش روزانه بیش از یک‌بار ارسال می‌شود»): اگر همین
                    # دورِ خطاخورده قبل از رسیدن به این خطا موفق شده بود پرچم «امروز پردازش/
                    # گزارش شد» (یا هر پنجره‌ی دیگر tf_windows) را mark+save+push کند، آن
                    # revert بالا آن پیشرفت را فقط از حافظه پاک نمی‌کند - چون چند خط پایین‌تر
                    # save_state(state) همین نسخه‌ی reverted را دوباره روی دیسک می‌نویسد و
                    # کامیت دوره‌ای بعدی آن را به ریموت هم پوش می‌کند و پرچمِ واقعاً ارسال‌شده
                    # را از بین می‌برد. این خط آن پرچم‌ها را از همان فایل دیسکی که run_scan_cycle
                    # خودش قبل از خطا به‌روزرسانی و پوش کرده بود، دوباره در state بازیابی می‌کند.
                    _refresh_tf_windows_from_disk(state)
                last_scan = time.time()
                save_state(state)  # بعد از هر دور اسکن کامل، ذخیره‌ی فوری روی دیسک

            if time.time() - last_commit >= GIT_COMMIT_EVERY_SECONDS:
                save_state(state)
                git_commit_and_push()
                last_commit = time.time()

            time.sleep(WS_CHECK_INTERVAL_SECONDS)
    except Exception as e:
        # طبق بازبینی برنامه‌نویس («یک مکانیسم try/except جامع در اطراف کل تابع»):
        # این حالا هم خطاهای واقعاً پیش‌بینی‌نشده‌ی داخل حلقه (خارج از سه try/except
        # داخلی که تک‌تک خطاهای معمول را می‌گیرند)، و هم خطاهای احتمالی *قبل* از رسیدن
        # به حلقه (load_state، بروزرسانی اولیه، start-کردن ترد WebSocket) را می‌گیرد.
        # حتی این حالت هم نباید باعث شود آخرین state سالم ذخیره/پوش نشود، و باید فوراً
        # به ادمین اطلاع داده شود - چون بدون این هشدار، تنها نشانه‌ی این خطا یک خط لاگ
        # در Actions است که معمولاً کسی نمی‌بیند.
        logger.error(f"🔥 خطای بحرانی و پیش‌بینی‌نشده در main() - وضعیت با امنیت ذخیره می‌شود: {e}")
        notify_admin(
            f"خطای بحرانی و پیش‌بینی‌نشده در bot.py باعث پایان زودهنگام این اجرا شد:\n{e}\n\n"
            f"وضعیت با امنیت ذخیره/پوش شد. طبق زمان‌بندی جدید cron، اجرای بعدی طی چند "
            f"دقیقه‌ی آینده خودکار شروع می‌شود.",
            state, kind="critical_main_error",
        )
    finally:
        stop_event.set()
        try:
            save_state(state)
            git_commit_and_push()
        except Exception as e:
            # 🔴 (تضمین «بدون قطعی») حتی اگر خودِ ذخیره/پوشِ نهایی هم خطا بدهد، نباید
            # اجازه بدهیم یک استثنای مدیریت‌نشده از finally بیرون بزند و کل پروسه را
            # با traceback خام crash کند - همان مقدار state ممکن که تا همین‌جا در
            # حافظه بود، در بهترین حالت روی دیسک محلی می‌ماند و اجرای بعدی (که هر حال
            # از ریموت fresh checkout می‌کند) کارش را ادامه می‌دهد.
            logger.error(f"❌ حتی ذخیره/پوش نهایی هم خطا داد: {e}")
        logger.info("✅ این نشست از حلقه تمام شد (اجرای زمان‌بندی‌شده‌ی بعدی طی چند دقیقه‌ی آینده بی‌وقفه ادامه می‌دهد)")


if __name__ == "__main__":
    main()
