import os, pickle, logging
import json
import time as time_mod
import pandas as pd
from datetime import datetime, date, time, timedelta
import threading
import queue
from pandas import Timestamp
import csv
import requests
from threading import Lock
import pytz
from threading import Event
import random
import sqlite3
from pymongo import MongoClient

# -- ATM / Straddle Configuration ----------------------------------------------
ATM_LEVELS_COUNT = max(1, 4)     # Levels above and below ATM
ATM_INTERVAL     = max(100, 200) # Points between ATM levels
STRANGLE_OFFSET  = max(100, 500) # CE/PE offset for strangle mode
USE_STRADDLE     = True          # True=straddle, False=strangle

IST = pytz.timezone('Asia/Kolkata')
data_thread_alive = Event()

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# -- Order / Trade Configuration ------------------------------------------------
STRATEGY_ID     = "SMG_STRD_BSE"
ORDER_ID_PREFIX = "SMG"
qty    = 20
markup = 10
clientID = 'DUMMY18'

# -- SlicedMango Period / Slot Configuration ------------------------------------
PERIOD_MINUTES       = 30   # Length of each trading period in minutes
MAX_SLOTS            = 3    # Maximum slot entries per level per period
MIN_CANDLES_REQUIRED = 10   # Minimum complete candles before VWAP is used for entries

# Price fields: (max_premium_inclusive, entry_pct, target_pct, sl_pct)
# entry_pct / target_pct can be a single float (uniform for all slots, multiplied by slot_num)
# OR a list of floats (one per slot, used directly as the threshold distance from VWAP).
# Percentages applied to vwap_ref value (locked at first slot entry per period).

# --- Per-slot config for vwap_ref > 200 (edit these to tune behaviour) ---
# Entry: how far below VWAP each slot triggers (slot 1..MAX_SLOTS)
SLOT_ENTRY_PCTS_HIGH  = [0.04, 0.09, 0.12]
# Target: how far above entry price each slot exits as profit (slot 1..MAX_SLOTS)
SLOT_TARGET_PCTS_HIGH = [0.025, 0.03, 0.03]

PRICE_FIELDS = [  # entry_pct,            target_pct,             sl_pct
    (100,          0.04,                   0.04,                   0.04),  # vwap_ref <= 100  -> uniform 4%
    (200,          0.02,                   0.02,                   0.02),  # 100 < vwap_ref <= 200 -> uniform 2%
    (float('inf'), SLOT_ENTRY_PCTS_HIGH,   SLOT_TARGET_PCTS_HIGH,  0.01),  # vwap_ref > 200  -> per-slot
]

# Per-slot stoploss
ENABLE_SLOT_SL = False

# Basket target / SL (total P&L across all active slots for a level, in points x qty)
ENABLE_BASKET_TARGET = True
BASKET_TARGET_POINTS = 3000   # Total P&L >= this -> close all slots for this level
ENABLE_BASKET_SL     = False
BASKET_SL_POINTS     = 1500   # Total loss >= this -> close all slots for this level

# Minimum combined premium to allow new slot entry
MIN_ENTRY_PREMIUM = 10.0

# -- OI-Based Exit Configuration ------------------------------------------------
# Reads OI signals from signals DB (same generator used by Full_Combo_70_Live_V1.py)
# For BUY strategy:
#   BEARISH OI (put buying > call buying): calls weakening -> exit/block ABOVE ATM (LV+1, LV+2...)
#   BULLISH OI (call buying > put buying): puts weakening  -> exit/block BELOW ATM (LV-1, LV-2...)
ENABLE_OI_EXIT   = False
SIGNALS_DB_DIR   = r"D:\PrakulEditDaily\OI_SignalGenerator\signals_output_1.2"

# -- Latency profiling ----------------------------------------------------------
# Set False before going live: removes all timing overhead and orders.csv timestamp column
LATENCY_PROFILING = True

# -- Data Source Configuration --------------------------------------------------
ZERODHA_ENABLED       = True
ALGOTEST_DB_DIR       = r"D:\\omshree\\AlgotestCSVData\\Algotest_Hybrid_DB_Data"
ZERODHA_MONGO_URI     = "mongodb://localhost:27017/"
ZERODHA_MONGO_DB      = "MarketWatch_Live"
BSEFO_MASTER_PATH     = r"D:\\omshree\\ZerodhaCSVData\\BSEFO.csv"
# Base dir for Zerodha CSV spot files: backend\DDMMYYYY\SPOT\SENSEX\YYYYMMDD_SENSEX_SPOT.csv
SENSEX_SPOT_CSV_BASE  = r"D:\\omshree\\ZerodhaCSVData"

# Path to orders.csv consumed by the live trading engine
ORDERS_CSV_PATH = r"D:\PrakulEditDaily\Paper_All_Versions\Working_Programs_Using_MongoQuoteMode\Sliced_Mango_Buy_4_Percent_Main_10AM-12pm_30minVWAP\orders.csv"

# -- Global State ---------------------------------------------------------------
INITIAL_SPOT                 = None
FIXED_ATM_LEVELS             = {}
ACTIVE_STRIKE_PAIRS          = {}
eligibility_lock             = None
session                      = None
last_eligibility_update_time = None
eligibility_queue            = None
eligibility_thread_alive     = None
zerodha_reader               = None

# OI signal globals
OI_SIGNAL_STATE   = None   # 'BULLISH', 'BEARISH', or None
LAST_OI_CHECK_TIME = None
OI_CURRENT_DATA   = None

# Latency logger (set in main() when LATENCY_PROFILING=True)
latency_logger = None

# Slot event queue: trading thread puts data here, background thread writes CSV.
# maxsize=1000 is far more than a full trading day ever needs.
_slot_event_queue = queue.Queue(maxsize=1000)


def _lat(label, elapsed_ms, **extra):
    """Write one latency record to smg_latency.log.  No-op when profiling is off."""
    if not LATENCY_PROFILING or latency_logger is None:
        return
    parts = [f"{label}: {elapsed_ms:.3f}ms"]
    for k, v in extra.items():
        parts.append(f"{k}={v:.1f}" if isinstance(v, float) else f"{k}={v}")
    latency_logger.info(" | ".join(parts))


# -- Algotest DB helpers --------------------------------------------------------

def get_algotest_db_path(symbol='SENSEX'):
    today_str = datetime.now(IST).strftime('%Y%m%d')
    return os.path.join(ALGOTEST_DB_DIR, f"{symbol}_{today_str}.db")

_algotest_db_conn = None
_algotest_db_lock = Lock()

def _get_algotest_db_conn():
    global _algotest_db_conn
    if _algotest_db_conn is None:
        db_path = get_algotest_db_path('SENSEX')
        db_uri  = f"file:{db_path}?mode=ro"
        _algotest_db_conn = sqlite3.connect(db_uri, uri=True, timeout=30.0, check_same_thread=False)
    return _algotest_db_conn

def load_latest_from_db(symbol, strike, opt_type):
    try:
        with _algotest_db_lock:
            conn   = _get_algotest_db_conn()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT token, open, high, low, close, vol_in_day, bar_timestamp "
                "FROM latest_prices WHERE strike = ? AND opt_type = ?",
                (int(strike), opt_type)
            )
            row = cursor.fetchone()
        if not row:
            return None
        token, open_p, high_p, low_p, close_p, volume, bar_timestamp = row
        dt     = datetime.strptime(bar_timestamp, "%Y-%m-%dT%H:%M:%S")
        ts_ist = IST.localize(dt) - timedelta(minutes=1)
        return {
            "token":     token,
            "open":      float(open_p),
            "high":      float(high_p),
            "low":       float(low_p),
            "close":     float(close_p),
            "volume":    int(volume) if volume is not None else 0,
            "timestamp": ts_ist,
        }
    except Exception as e:
        logging.error(f"DB load error {symbol} {strike} {opt_type}: {e}")
        return None

def convert_token_to_exchange_id(algotest_token):
    if isinstance(algotest_token, str):
        if algotest_token.startswith('BSE_'):
            return int(algotest_token.replace('BSE_', ''))
        return int(algotest_token)
    return int(algotest_token)

# -- Spot price -----------------------------------------------------------------

def get_sensex_spot_from_csv():
    """Read latest SENSEX spot price from local Zerodha spot CSV.
    File: SENSEX_SPOT_CSV_BASE/backend/DDMMYYYY/SPOT/SENSEX/YYYYMMDD_SENSEX_SPOT.csv
    Uses binary seek to read only the last line - ~0.1ms, no network dependency.
    Returns float spot price, or None if file missing/unreadable.
    """
    try:
        now    = datetime.now(IST)
        folder = now.strftime('%d%m%Y')
        fname  = now.strftime('%Y%m%d') + '_SENSEX_SPOT.csv'
        path   = os.path.join(SENSEX_SPOT_CSV_BASE, 'backend', folder, 'SPOT', 'SENSEX', fname)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            # Seek to near end to find last line without loading whole file
            try:
                f.seek(-2, os.SEEK_END)
            except OSError:
                return None  # file too small (header only)
            while f.read(1) != b'\n':
                try:
                    f.seek(-2, os.SEEK_CUR)
                except OSError:
                    f.seek(0)
                    break
            last = f.readline().decode('utf-8', errors='ignore').strip()
        if not last or ',' not in last:
            return None
        parts = last.split(',')
        if parts[0].startswith('recv'):   # skip header row if file is tiny
            return None
        spot = float(parts[1])
        return spot if spot > 0 else None
    except Exception:
        return None


def get_sensex_spot_bse1(session, keep_trying=False):
    url     = "https://prices.algotest.in/historical"
    attempt = 0
    while True:
        attempt += 1
        now   = datetime.now(IST)
        today = now.date()
        try:
            if   attempt == 1: start = datetime.combine(today, time(9, 15))
            elif attempt == 2: start = now - timedelta(minutes=5)
            elif attempt == 3: start = now - timedelta(minutes=15)
            elif attempt == 4: start = now - timedelta(minutes=30)
            else:              start = now - timedelta(minutes=60)
            params = {
                "tokens":   "BSE_1",
                "start_dt": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_dt":   now.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            _t = time_mod.perf_counter()
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            _lat("SPOT_HTTP", (time_mod.perf_counter() - _t) * 1000, attempt=attempt)
            data       = resp.json().get("BSE_1", {})
            closes     = data.get("close", [])
            timestamps = data.get("timestamp", [])
            if closes and timestamps:
                return float(closes[-1]), timestamps[-1]
        except Exception as e:
            logging.error(f"Spot fetch attempt {attempt} failed: {e}")
        if not keep_trying and attempt >= 5:
            return None, None
        wait = 5 if attempt <= 3 else (15 if attempt <= 10 else 30)
        time_mod.sleep(wait)

# -- ATM level setup ------------------------------------------------------------

def initialize_fixed_atm_levels():
    global INITIAL_SPOT, FIXED_ATM_LEVELS, ACTIVE_STRIKE_PAIRS
    logging.info("Waiting for SENSEX spot price...")
    # Try local CSV first (instant); fall back to Algotest HTTP (keep_trying=True)
    spot = get_sensex_spot_from_csv()
    if spot is not None:
        logging.info(f"Got spot from CSV: {spot}")
    else:
        logging.info("CSV spot unavailable, falling back to Algotest HTTP...")
        spot, timestamp = get_sensex_spot_bse1(session, keep_trying=True)
        if spot is None:
            raise RuntimeError("Unexpected failure in spot price fetching")
        logging.info(f"Got spot from Algotest: {spot} ({timestamp})")
    INITIAL_SPOT = spot
    initial_atm  = round(spot / ATM_INTERVAL) * ATM_INTERVAL
    FIXED_ATM_LEVELS.clear()
    ACTIVE_STRIKE_PAIRS.clear()
    for i in range(-ATM_LEVELS_COUNT, ATM_LEVELS_COUNT + 1):
        level_atm     = initial_atm + (i * ATM_INTERVAL)
        level_name    = f"ATM_LV{i:+d}"
        ce_strike     = level_atm if USE_STRADDLE else level_atm + STRANGLE_OFFSET
        pe_strike     = level_atm if USE_STRADDLE else level_atm - STRANGLE_OFFSET
        indicator_key = f"{ce_strike}_STRADDLE" if USE_STRADDLE else f"{ce_strike}CE_{pe_strike}PE"
        FIXED_ATM_LEVELS[level_name] = {
            'atm': level_atm, 'ce_strike': ce_strike,
            'pe_strike': pe_strike, 'strike_pair_key': indicator_key,
        }
        ACTIVE_STRIKE_PAIRS[level_name] = {
            'ce_strike':      ce_strike,
            'pe_strike':      pe_strike,
            'is_active':      True,
            'can_enter_trade':abs(i) <= 1,
            'last_signal':    None,
            'indicator_key':  indicator_key,
            'atm_level':      level_atm,
            'level_index':    i,
        }
    logging.info("Fixed ATM levels initialised:")
    for ln, ld in FIXED_ATM_LEVELS.items():
        status = "TRADE_ELIGIBLE" if ACTIVE_STRIKE_PAIRS[ln]['can_enter_trade'] else "CALC_ONLY"
        logging.info(f"  {ln}: ATM={ld['atm']} CE={ld['ce_strike']} PE={ld['pe_strike']} [{status}]")
    return FIXED_ATM_LEVELS

# -- Eligibility background processor ------------------------------------------

def eligibility_background_processor():
    global eligibility_thread_alive, eligibility_queue
    eligibility_thread_alive.set()
    try:
        while True:
            try:
                current_spot = eligibility_queue.get(timeout=10)
                _update_eligibility_internal(current_spot)
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Eligibility BG error: {e}")
    except Exception as e:
        logging.error(f"Eligibility BG crashed: {e}")
    finally:
        eligibility_thread_alive.clear()

def update_trade_eligibility_by_spot(current_spot):
    global eligibility_queue
    if eligibility_queue is not None:
        try:
            eligibility_queue.put_nowait(current_spot)
        except queue.Full:
            pass

def _update_eligibility_internal(current_spot):
    global ACTIVE_STRIKE_PAIRS, last_eligibility_update_time, eligibility_lock
    now = datetime.now(IST)
    if last_eligibility_update_time is not None:
        if (now - last_eligibility_update_time).total_seconds() < 5:
            return
    last_eligibility_update_time = now
    with eligibility_lock:
        if not ACTIVE_STRIKE_PAIRS:
            return
        current_atm = round(current_spot / ATM_INTERVAL) * ATM_INTERVAL
        distances   = sorted(
            [(ln, pd_['atm_level'], abs(pd_['atm_level'] - current_atm))
             for ln, pd_ in ACTIVE_STRIKE_PAIRS.items()],
            key=lambda x: x[2]
        )
        closest_3 = {item[0] for item in distances[:3]}
        for ln, pd_ in ACTIVE_STRIKE_PAIRS.items():
            pd_['can_enter_trade'] = (ln in closest_3)

# -- OI Signal Functions --------------------------------------------------------

def check_oi_signal_and_update():
    """
    Read OI signal from signals DB and update OI_SIGNAL_STATE global.
    For buy strategy:
      net_oi_change > 0 (BULLISH): more call OI building -> puts weakening -> exit/block BELOW ATM
      net_oi_change < 0 (BEARISH): more put OI building  -> calls weakening -> exit/block ABOVE ATM
    """
    global OI_SIGNAL_STATE, LAST_OI_CHECK_TIME, OI_CURRENT_DATA

    if not ENABLE_OI_EXIT:
        return None

    try:
        today_str       = datetime.now(IST).strftime('%Y%m%d')
        signals_db_path = os.path.join(SIGNALS_DB_DIR, f"signals_SENSEX_{today_str}.db")

        if not os.path.exists(signals_db_path):
            logging.warning(f"OI EXIT ENABLED but signals DB not found: {signals_db_path}")
            logging.warning("Ensure OI signal generator is running with SYMBOL='SENSEX'.")
            LAST_OI_CHECK_TIME = datetime.now(IST)
            return None

        signals_db_uri = f"file:{signals_db_path}?mode=ro"
        _oi_t = time_mod.perf_counter()
        conn   = sqlite3.connect(signals_db_uri, uri=True, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT signal_value, net_oi_change, call_oi_change, put_oi_change, timestamp
            FROM signal_unwinding
            ORDER BY id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        conn.close()
        _oi_db_ms = (time_mod.perf_counter() - _oi_t) * 1000

        if not row:
            logging.warning("OI EXIT ENABLED but no signal data yet in signals DB.")
            LAST_OI_CHECK_TIME = datetime.now(IST)
            return None

        signal_value, net_oi_change, call_oi_change, put_oi_change, timestamp = row
        try:
            sig_dt = datetime.fromisoformat(str(timestamp))
            if sig_dt.tzinfo is None:
                sig_dt = IST.localize(sig_dt)
            _sig_age_ms = (datetime.now(IST) - sig_dt).total_seconds() * 1000
        except Exception:
            _sig_age_ms = -1.0
        _lat("OI_CHECK", _oi_db_ms, signal_age_ms=_sig_age_ms, net_oi=net_oi_change)

        new_state = None
        if net_oi_change > 0:
            new_state = 'BULLISH'
        elif net_oi_change < 0:
            new_state = 'BEARISH'

        logging.info("=" * 60)
        if new_state != OI_SIGNAL_STATE:
            logging.info(f"OI SIGNAL CHANGED: {OI_SIGNAL_STATE} -> {new_state}")
        else:
            logging.info(f"OI SIGNAL UPDATE: {new_state if new_state else 'NEUTRAL'}")
        logging.info(f"  Signal Timestamp: {timestamp}")
        logging.info(f"  Call OI Change:   {call_oi_change:+,}")
        logging.info(f"  Put OI Change:    {put_oi_change:+,}")
        logging.info(f"  Net OI Change:    {net_oi_change:+,}")
        if new_state == 'BULLISH':
            logging.info("  Action: Block/exit trades BELOW ATM (puts weakening for buy longs)")
        elif new_state == 'BEARISH':
            logging.info("  Action: Block/exit trades ABOVE ATM (calls weakening for buy longs)")
        else:
            logging.info("  Action: No OI-based restrictions")
        logging.info("=" * 60)

        OI_CURRENT_DATA = {
            'call_oi_change':  call_oi_change,
            'put_oi_change':   put_oi_change,
            'oi_signal_value': net_oi_change,
        }
        OI_SIGNAL_STATE    = new_state
        LAST_OI_CHECK_TIME = datetime.now(IST)
        return new_state

    except Exception as e:
        logging.error(f"Error checking OI signal: {e}", exc_info=True)
        return None


def should_oi_force_exit(level_name, oi_signal_state):
    """
    Returns True if OI signal indicates this level should be force-exited (BUY strategy).
    BEARISH OI (put buying > call buying) -> calls weakening -> exit ABOVE ATM (LV+1, +2, +3)
    BULLISH OI (call buying > put buying) -> puts weakening  -> exit BELOW ATM (LV-1, -2, -3)
    """
    if not ENABLE_OI_EXIT or oi_signal_state is None:
        return False
    try:
        level_index = int(level_name.split('ATM_LV')[1])
    except (IndexError, ValueError):
        return False
    if oi_signal_state == 'BEARISH' and level_index > 0:
        return True
    if oi_signal_state == 'BULLISH' and level_index < 0:
        return True
    return False


def should_oi_block_entry(level_name, oi_signal_state):
    """
    Returns True if OI signal indicates new slot entries should be blocked (BUY strategy).
    Same direction logic as force exit.
    """
    if not ENABLE_OI_EXIT or oi_signal_state is None:
        return False
    try:
        level_index = int(level_name.split('ATM_LV')[1])
    except (IndexError, ValueError):
        return False
    if oi_signal_state == 'BEARISH' and level_index > 0:
        return True
    if oi_signal_state == 'BULLISH' and level_index < 0:
        return True
    return False

# -- Zerodha MongoDB reader -----------------------------------------------------

class ZerodhaMongoReader:
    def __init__(self, mongo_uri, db_name):
        self.client           = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db               = self.client[db_name]
        self.collection_name  = f"ticks_{datetime.now(IST).strftime('%Y%m%d')}"
        self.collection       = self.db[self.collection_name]
        self.strike_cache     = {}
        self.last_refresh     = 0
        self.refresh_interval = 0.5
        self.lock             = Lock()
        self.cache_size       = 0

    def get_snapshot_path(self):
        return f"MongoDB:{self.db.name}.{self.collection_name}"

    def refresh_cache(self, force=False):
        now_t = time_mod.time()
        if not force and (now_t - self.last_refresh) < self.refresh_interval:
            return True
        try:
            cutoff   = datetime.now(IST) - timedelta(seconds=30)
            pipeline = [
                {"$match":  {"timestamp": {"$gte": cutoff}, "category": "OPTIONS"}},
                {"$sort":   {"timestamp": -1}},
                {"$group":  {
                    "_id":              {"name": "$name", "strike": "$strike", "instrument_type": "$instrument_type"},
                    "last_price":       {"$first": "$last_price"},
                    "timestamp":        {"$first": "$timestamp"},
                    "instrument_token": {"$first": "$instrument_token"},
                }},
            ]
            _agg_t = time_mod.perf_counter()
            results = list(self.collection.aggregate(pipeline))
            _agg_ms = (time_mod.perf_counter() - _agg_t) * 1000
            with self.lock:
                self.strike_cache.clear()
                for doc in results:
                    name      = doc["_id"]["name"]
                    strike    = doc["_id"]["strike"]
                    inst_type = doc["_id"]["instrument_type"]
                    if strike is not None and inst_type in ('CE', 'PE'):
                        self.strike_cache[(name, int(strike), inst_type)] = {
                            "last_price":       doc["last_price"],
                            "timestamp":        doc["timestamp"],
                            "instrument_token": doc.get("instrument_token"),
                        }
                self.cache_size   = len(self.strike_cache)
                self.last_refresh = time_mod.time()
                # Compute data staleness: age of newest tick in cache
                if LATENCY_PROFILING and self.cache_size > 0:
                    try:
                        newest_ts = max(v["timestamp"] for v in self.strike_cache.values())
                        now_ist = datetime.now(IST)
                        if newest_ts.tzinfo is None:
                            newest_ts = pytz.utc.localize(newest_ts)  # PyMongo returns naive UTC
                        _data_age_ms = max(0.0, (now_ist - newest_ts).total_seconds() * 1000)
                    except Exception:
                        _data_age_ms = -1.0
                    _lat("ZERODHA_REFRESH", _agg_ms, size=self.cache_size, data_age_ms=_data_age_ms)
            if force:
                logging.info(f"Zerodha cache refreshed: {self.cache_size} instruments")
            return self.cache_size > 0 or force
        except Exception as e:
            logging.error(f"Zerodha cache refresh error: {e}")
            return False

    def get_ltp_by_strike(self, symbol, strike, opt_type):
        with self.lock:
            data = self.strike_cache.get((symbol, int(strike), opt_type))
            return data["last_price"] if data else None

    def get_ltp_and_age(self, symbol, strike, opt_type):
        """Returns (last_price, data_age_ms) where data_age_ms is age of the tick.
        Returns (None, None) if not in cache."""
        with self.lock:
            data = self.strike_cache.get((symbol, int(strike), opt_type))
            if data is None:
                return None, None
            ltp = data["last_price"]
            ts  = data.get("timestamp")
            if ts is not None:
                try:
                    now_ist = datetime.now(IST)
                    if ts.tzinfo is None:
                        ts = pytz.utc.localize(ts)  # PyMongo returns naive UTC
                    age_ms = max(0.0, (now_ist - ts).total_seconds() * 1000)
                except Exception:
                    age_ms = -1.0
            else:
                age_ms = -1.0
            return ltp, age_ms

    def get_cache_age_ms(self):
        """Milliseconds since last cache refresh."""
        if self.last_refresh == 0:
            return float('inf')
        return (time_mod.time() - self.last_refresh) * 1000

    def get_cache_stats(self):
        with self.lock:
            return {
                'cache_size':    self.cache_size,
                'last_refresh':  datetime.fromtimestamp(self.last_refresh).strftime('%H:%M:%S') if self.last_refresh else 'Never',
                'snapshot_path': self.get_snapshot_path(),
            }

# -- Candle tracking ------------------------------------------------------------

strike_pair_candles = {}
candle_lock         = Lock()

def get_candle_pickle_path():
    today = datetime.now(IST).strftime("%Y-%m-%d")
    os.makedirs(f"candle_data/{today}", exist_ok=True)
    return f"candle_data/{today}/smg_candles.pkl"

def load_candles():
    global strike_pair_candles
    path = get_candle_pickle_path()
    with candle_lock:
        try:
            with open(path, 'rb') as f:
                loaded = pickle.load(f)
            if isinstance(loaded, dict):
                if '_volume_cache_' in loaded:
                    strike_pair_candles = loaded['candles']
                    return loaded['_volume_cache_']
                else:
                    strike_pair_candles = loaded
                    return {}
        except FileNotFoundError:
            strike_pair_candles = {}
        except Exception as e:
            logging.error(f"Candle load error: {e}. Starting fresh.")
            strike_pair_candles = {}
    return {}

def save_candles(volume_cache=None):
    path = get_candle_pickle_path()
    with candle_lock:
        try:
            tmp  = path + '.tmp'
            data = {'candles': strike_pair_candles, '_volume_cache_': volume_cache or {}}
            with open(tmp, 'wb') as f:
                pickle.dump(data, f)
                f.flush()
            if os.name == 'nt' and os.path.exists(path):
                os.remove(path)
            os.rename(tmp, path)
        except Exception as e:
            logging.error(f"Candle save error: {e}")

def update_candles_by_indicator_key(strike_pair_data, fetcher):
    global strike_pair_candles
    if strike_pair_data is None or strike_pair_data.empty:
        return
    with candle_lock:
        current_time = datetime.now(IST)
        current_date = current_time.date()
        for _, row in strike_pair_data.iterrows():
            ik      = row['IndicatorKey']
            open_p  = row['Combined_Open']
            high_p  = row['Combined_High']
            low_p   = row['Combined_Low']
            close_p = row['Combined_Close']
            cum_vol = row['Combined_Volume']
            if ik not in strike_pair_candles:
                strike_pair_candles[ik] = {}
            if current_date not in strike_pair_candles[ik]:
                strike_pair_candles[ik][current_date] = {}
            if '1min' not in strike_pair_candles[ik][current_date]:
                strike_pair_candles[ik][current_date]['1min'] = {}
            cache_key    = (ik, row['Expiry'])
            last_vol     = fetcher.last_volume_cache.get(cache_key, 0)
            incr_vol     = max(0, cum_vol - last_vol)
            fetcher.last_volume_cache[cache_key] = cum_vol
            minute_ts    = row['Timestamp'].replace(second=0, microsecond=0)
            candle_end   = minute_ts + timedelta(minutes=1)
            candles_dict = strike_pair_candles[ik][current_date]['1min']
            if minute_ts not in candles_dict:
                candles_dict[minute_ts] = {
                    'open': open_p, 'high': high_p, 'low': low_p, 'close': close_p,
                    'volume': incr_vol, 'complete': current_time >= candle_end, 'end_time': candle_end,
                }
            else:
                c = candles_dict[minute_ts]
                c['high']     = max(c['high'],  high_p)
                c['low']      = min(c['low'],   low_p)
                c['close']    = close_p
                c['volume']  += incr_vol
                c['complete'] = current_time >= candle_end
            # Mark old incomplete candles as complete (iterate keys list to avoid mutation issues)
            for ts in list(candles_dict.keys()):
                if current_time >= candles_dict[ts]['end_time']:
                    candles_dict[ts]['complete'] = True

# -- VWAP calculation -----------------------------------------------------------

def calculate_anchored_vwap(candles):
    """Anchored VWAP from complete candles (uses close x volume as price-volume proxy)."""
    cum_pv, cum_vol = 0.0, 0.0
    vwap_values     = []
    for ts in sorted(candles.keys()):
        c = candles[ts]
        if not c.get('complete', False):
            continue
        vol = max(0, c['volume'])
        cum_pv  += c['close'] * vol
        cum_vol += vol
        if cum_vol > 0:
            vwap_values.append(cum_pv / cum_vol)
    return vwap_values

# -- Market timings -------------------------------------------------------------

def get_market_timings():
    now = datetime.now(IST)
    return {
        'market_open':      now.replace(hour=9,  minute=15, second=10, microsecond=0),
        'first_trade_start':now.replace(hour=10,  minute=00, second=0,  microsecond=0),
        'last_trade_entry': now.replace(hour=11, minute=50, second=0,  microsecond=0),
        'force_exit_time':  now.replace(hour=12, minute=00, second=0,  microsecond=0),
        'kill_time':        now.replace(hour=12, minute=1, second=30, microsecond=0),
    }

# -- Period helper --------------------------------------------------------------

def get_period_end(now, first_trade_start):
    """Return the end datetime of the current PERIOD_MINUTES-grid period."""
    delta_secs = (now - first_trade_start).total_seconds()
    if delta_secs < 0:
        return first_trade_start + timedelta(minutes=PERIOD_MINUTES)
    period_num   = int(delta_secs / (PERIOD_MINUTES * 60))
    period_start = first_trade_start + timedelta(minutes=period_num * PERIOD_MINUTES)
    return period_start + timedelta(minutes=PERIOD_MINUTES)

# -- Price field helper ---------------------------------------------------------

def get_price_field(vwap_ref):
    """Return (entry_pct, target_pct, sl_pct) determined by vwap_ref value."""
    for max_premium, entry_pct, target_pct, sl_pct in PRICE_FIELDS:
        if vwap_ref <= max_premium:
            return entry_pct, target_pct, sl_pct
    return 0.01, 0.01, 0.01  # fallback (float('inf') row always catches)

# -- Order writing --------------------------------------------------------------

orders_csv_lock = Lock()

def write_orders_batch(orders_list, max_retries=10, retry_delay=0.2):
    """Write a batch of orders to orders.csv with retry. Returns True on success."""
    if not orders_list:
        return True
    for attempt in range(max_retries):
        try:
            _w_t = time_mod.perf_counter()
            with orders_csv_lock:
                with open(ORDERS_CSV_PATH, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # LATENCY_PROFILING: append write timestamp as last column.
                    # Remove this (and set LATENCY_PROFILING=False) before live trading.
                    _ts_str = datetime.now(IST).strftime('%H:%M:%S.%f') if LATENCY_PROFILING else None
                    for order_id, token, value, price, quantity, strategy_id in orders_list:
                        row = [order_id, token, 'L', value, price, quantity, strategy_id]
                        if _ts_str:
                            row.append(_ts_str)
                        writer.writerow(row)
                    f.flush()
            _lat("ORDERS_WRITE", (time_mod.perf_counter() - _w_t) * 1000,
                 count=len(orders_list))
            logging.info(f"Batch: {len(orders_list)} orders written")
            for oid, tok, val, prc, _, _ in orders_list:
                logging.info(f"  - {oid} | Token:{tok} | Value:{val} | Price:{prc}")
            return True
        except (PermissionError, BlockingIOError, OSError) as e:
            if attempt < max_retries - 1:
                sleep_t = retry_delay * (2 ** attempt) + (0.1 * attempt)
                logging.warning(f"Batch write attempt {attempt+1}/{max_retries} failed: {e}. Retry in {sleep_t:.2f}s")
                time_mod.sleep(sleep_t)
            else:
                logging.error(f"CRITICAL: Batch write failed after {max_retries} attempts: {e}")
                return False
        except Exception as e:
            logging.error(f"Unexpected batch write error: {e}")
            return False
    return False

# -- Slot event CSV logging -----------------------------------------------------

def log_slot_event(event_type, basket, slots, combined_ltp, ce_ltp, pe_ltp,
                   reason='', pnl=None):
    """Append slot entry/exit events to daily CSV for post-analysis."""
    today_str   = datetime.now(IST).strftime('%Y%m%d')
    csv_path    = f"smg_slots_{today_str}.csv"
    headers     = [
        'datetime', 'event_type', 'level_name', 'ce_strike', 'pe_strike',
        'slot_num', 'basket_id', 'combined_ltp', 'ce_ltp', 'pe_ltp',
        'entry_price', 'target_price', 'sl_price', 'pnl', 'reason',
        'vwap_ref', 'price_field_pct',
    ]
    file_exists = os.path.isfile(csv_path)
    now_str     = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    pf_pct      = basket.price_field[0] if basket.price_field else ''
    try:
        with open(csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(headers)
            for slot in slots:
                # Compute per-slot P&L if not explicitly provided
                if pnl is None and slot.entry_price:
                    slot_pnl = round((combined_ltp - slot.entry_price) * qty, 4)
                else:
                    slot_pnl = round(pnl, 4) if pnl is not None else ''
                writer.writerow([
                    now_str, event_type, basket.level_name,
                    basket.ce_strike, basket.pe_strike,
                    slot.slot_num, slot.basket_counter,
                    round(combined_ltp, 4), round(ce_ltp, 4), round(pe_ltp, 4),
                    round(slot.entry_price, 4) if slot.entry_price else '',
                    round(slot.target_price, 4) if slot.target_price else '',
                    round(slot.sl_price, 4) if slot.sl_price else '',
                    slot_pnl, reason,
                    round(basket.vwap_ref, 4) if basket.vwap_ref else '',
                    pf_pct,
                ])
    except Exception as e:
        logging.error(f"Slot event CSV log error: {e}")


def _queue_slot_event(event_type, basket, slots, combined_ltp, ce_ltp, pe_ltp,
                      reason='', pnl=None):
    """Capture all slot event data NOW (in the trading thread) and hand off to
    the background writer.  Runs in ~1 microsecond - no file I/O on the hot path.
    All values are extracted immediately so there is no risk of the basket/slot
    objects being mutated before the background thread writes them.
    """
    try:
        now_str = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
        pf_pct  = basket.price_field[0] if basket.price_field else ''
        today_str = datetime.now(IST).strftime('%Y%m%d')
        records = []
        for slot in slots:
            slot_pnl = (
                round((combined_ltp - slot.entry_price) * qty, 4)
                if pnl is None and slot.entry_price
                else (round(pnl, 4) if pnl is not None else '')
            )
            records.append([
                now_str, event_type, basket.level_name,
                basket.ce_strike, basket.pe_strike,
                slot.slot_num, slot.basket_counter,
                round(combined_ltp, 4), round(ce_ltp, 4), round(pe_ltp, 4),
                round(slot.entry_price, 4) if slot.entry_price else '',
                round(slot.target_price, 4) if slot.target_price else '',
                round(slot.sl_price, 4) if slot.sl_price else '',
                slot_pnl, reason,
                round(basket.vwap_ref, 4) if basket.vwap_ref else '',
                pf_pct,
            ])
        _slot_event_queue.put_nowait((f"smg_slots_{today_str}.csv", records))
    except Exception:
        pass  # never block the trading thread


def _slot_event_writer_loop():
    """Background thread: drains _slot_event_queue and writes rows to smg_slots CSV.
    Completely off the trading critical path - GUI reads this file every 2s anyway.
    """
    _headers = [
        'datetime', 'event_type', 'level_name', 'ce_strike', 'pe_strike',
        'slot_num', 'basket_id', 'combined_ltp', 'ce_ltp', 'pe_ltp',
        'entry_price', 'target_price', 'sl_price', 'pnl', 'reason',
        'vwap_ref', 'price_field_pct',
    ]
    while True:
        try:
            csv_path, records = _slot_event_queue.get(timeout=5)
            file_exists = os.path.isfile(csv_path)
            with open(csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(_headers)
                writer.writerows(records)
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Slot event writer error: {e}")


# -- SlotInfo -------------------------------------------------------------------

class SlotInfo:
    __slots__ = [
        'slot_num', 'is_active', 'is_consumed',
        'entry_price', 'entry_ce_ltp', 'entry_pe_ltp', 'entry_time',
        'basket_counter', 'target_price', 'sl_price',
    ]

    def __init__(self, slot_num):
        self.slot_num       = slot_num
        self.is_active      = False
        self.is_consumed    = False
        self.entry_price    = 0.0
        self.entry_ce_ltp   = 0.0
        self.entry_pe_ltp   = 0.0
        self.entry_time     = None
        self.basket_counter = 0   # engine basket_counter at time of THIS slot's entry
        self.target_price   = 0.0
        self.sl_price       = 0.0

    def to_dict(self):
        return {
            'slot_num':       self.slot_num,
            'is_active':      self.is_active,
            'is_consumed':    self.is_consumed,
            'entry_price':    self.entry_price,
            'entry_ce_ltp':   self.entry_ce_ltp,
            'entry_pe_ltp':   self.entry_pe_ltp,
            'entry_time':     self.entry_time.isoformat() if self.entry_time else None,
            'basket_counter': self.basket_counter,
            'target_price':   self.target_price,
            'sl_price':       self.sl_price,
        }

    @classmethod
    def from_dict(cls, d):
        s = cls(d['slot_num'])
        s.is_active      = d.get('is_active', False)
        s.is_consumed    = d.get('is_consumed', False)
        s.entry_price    = d.get('entry_price', 0.0)
        s.entry_ce_ltp   = d.get('entry_ce_ltp', 0.0)
        s.entry_pe_ltp   = d.get('entry_pe_ltp', 0.0)
        raw_et           = d.get('entry_time')
        s.entry_time     = datetime.fromisoformat(raw_et) if raw_et else None
        s.basket_counter = d.get('basket_counter', 0)
        s.target_price   = d.get('target_price', 0.0)
        s.sl_price       = d.get('sl_price', 0.0)
        return s

# -- LevelBasket ---------------------------------------------------------------

class LevelBasket:
    def __init__(self, level_name, ce_strike, pe_strike, indicator_key):
        self.level_name    = level_name
        self.ce_strike     = ce_strike
        self.pe_strike     = pe_strike
        self.indicator_key = indicator_key
        self.ce_token      = None  # set from DB data; stable after first population
        self.pe_token      = None

        # Period state
        self.current_period_end = None
        self.basket_id          = None   # engine basket_counter at FIRST lock for this period
        self.vwap_ref           = None   # locked VWAP reference for this period
        self.price_field        = None   # (entry_pct, target_pct, sl_pct) - locked
        self.candidate_vwap     = None   # updated every candle close until locked
        self.is_vwap_locked     = False

        self.slots = {n: SlotInfo(n) for n in range(1, MAX_SLOTS + 1)}

    def reset_period(self, new_period_end):
        self.current_period_end = new_period_end
        self.basket_id          = None
        self.vwap_ref           = None
        self.price_field        = None
        self.is_vwap_locked     = False
        # candidate_vwap kept - will be refreshed from candle data
        for slot in self.slots.values():
            slot.is_active    = False
            slot.is_consumed  = False
            slot.entry_price  = slot.entry_ce_ltp = slot.entry_pe_ltp = 0.0
            slot.entry_time   = None
            slot.target_price = slot.sl_price = 0.0

    def get_active_slots(self):
        return [s for s in self.slots.values() if s.is_active]

    def get_basket_pnl(self, current_combined_ltp):
        return sum(
            (current_combined_ltp - s.entry_price) * qty
            for s in self.get_active_slots()
        )

    def to_dict(self):
        pf = list(self.price_field) if self.price_field else None
        return {
            'level_name':         self.level_name,
            'ce_strike':          self.ce_strike,
            'pe_strike':          self.pe_strike,
            'indicator_key':      self.indicator_key,
            'ce_token':           self.ce_token,
            'pe_token':           self.pe_token,
            'current_period_end': self.current_period_end.isoformat() if self.current_period_end else None,
            'basket_id':          self.basket_id,
            'vwap_ref':           self.vwap_ref,
            'price_field':        pf,
            'candidate_vwap':     self.candidate_vwap,
            'is_vwap_locked':     self.is_vwap_locked,
            'slots':              {str(n): s.to_dict() for n, s in self.slots.items()},
        }

    @classmethod
    def from_dict(cls, d):
        b = cls(d['level_name'], d['ce_strike'], d['pe_strike'], d['indicator_key'])
        b.ce_token  = d.get('ce_token')
        b.pe_token  = d.get('pe_token')
        raw_pe      = d.get('current_period_end')
        b.current_period_end = datetime.fromisoformat(raw_pe) if raw_pe else None
        b.basket_id          = d.get('basket_id')
        b.vwap_ref           = d.get('vwap_ref')
        pf                   = d.get('price_field')
        b.price_field        = tuple(pf) if pf else None
        b.candidate_vwap     = d.get('candidate_vwap')
        b.is_vwap_locked     = d.get('is_vwap_locked', False)
        b.slots = {
            int(n): SlotInfo.from_dict(sd)
            for n, sd in d.get('slots', {}).items()
        }
        return b

# -- SlicedMangoEngine ----------------------------------------------------------

class SlicedMangoEngine:
    def __init__(self):
        self.state_dir      = "trading_states_slicedmango"
        os.makedirs(self.state_dir, exist_ok=True)
        self.basket_counter = 0   # global monotonic counter; increments on each new basket lock
        self.baskets        = {}  # level_name -> LevelBasket
        self._state_dirty   = False  # set True after trade events; saved once per main loop cycle
        self.load_state()

    # -- Initialisation ---------------------------------------------------------

    def initialize_baskets(self):
        """Create LevelBasket for every active strike pair (once, after ATM init)."""
        with eligibility_lock:
            for level_name, pair in ACTIVE_STRIKE_PAIRS.items():
                if level_name not in self.baskets:
                    self.baskets[level_name] = LevelBasket(
                        level_name,
                        pair['ce_strike'],
                        pair['pe_strike'],
                        pair['indicator_key'],
                    )
        logging.info(f"SlicedMangoEngine: {len(self.baskets)} baskets initialised")

    # -- Candidate VWAP update --------------------------------------------------

    def update_candidate_vwaps(self):
        """Read latest candle data and update candidate_vwap for unlocked baskets."""
        date_val = datetime.now(IST).date()
        with candle_lock:
            for basket in self.baskets.values():
                if basket.is_vwap_locked:
                    continue
                ik       = basket.indicator_key
                day_data = strike_pair_candles.get(ik, {}).get(date_val)
                if not day_data:
                    continue
                candles  = day_data.get('1min', {})
                complete = [c for c in candles.values() if c.get('complete', False)]
                if len(complete) < MIN_CANDLES_REQUIRED:
                    continue
                vwap_vals = calculate_anchored_vwap(candles)
                if vwap_vals:
                    basket.candidate_vwap = vwap_vals[-1]

    # -- Token update -----------------------------------------------------------

    def update_tokens_from_options(self, options_df):
        """Populate ce_token / pe_token for each basket from latest DB options data."""
        if options_df is None or options_df.empty:
            return
        for level_name, basket in self.baskets.items():
            level_df = options_df[options_df['Level'] == level_name]
            if level_df.empty:
                continue
            ce_rows = level_df[level_df['Type'] == 'CE']
            pe_rows = level_df[level_df['Type'] == 'PE']
            if not ce_rows.empty:
                basket.ce_token = convert_token_to_exchange_id(ce_rows.iloc[0]['Instrument'])
            if not pe_rows.empty:
                basket.pe_token = convert_token_to_exchange_id(pe_rows.iloc[0]['Instrument'])

    # -- Main per-second cycle --------------------------------------------------

    def process_cycle(self):
        """
        Called once per second from the main loop.
        Per-level ordering:
          1. Period end       -> force exit all active slots, reset basket
          2. OI force exit    -> exit all active slots if OI adverse for this level
          3. Basket target/SL -> exit all active slots if basket P&L hits threshold
          4. Per-slot target/SL -> exit individual slots
          5. New slot entries  -> blocked by last_trade_entry, eligibility, OI, and VWAP guard
        """
        timings = get_market_timings()
        now     = datetime.now(IST)

        for level_name, basket in self.baskets.items():
            # Must have tokens from DB before we can write orders
            if basket.ce_token is None or basket.pe_token is None:
                continue

            # Fetch current Zerodha LTP for this level
            if zerodha_reader and LATENCY_PROFILING:
                ce_ltp, ce_age_ms = zerodha_reader.get_ltp_and_age("SENSEX", basket.ce_strike, "CE")
                pe_ltp, pe_age_ms = zerodha_reader.get_ltp_and_age("SENSEX", basket.pe_strike, "PE")
                _ltp_age_ms = max(
                    (a for a in (ce_age_ms, pe_age_ms) if a is not None and a >= 0),
                    default=-1.0
                )
            elif zerodha_reader:
                ce_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.ce_strike, "CE")
                pe_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.pe_strike, "PE")
                _ltp_age_ms = -1.0
            else:
                ce_ltp = pe_ltp = None
                _ltp_age_ms = -1.0
            if ce_ltp is None or pe_ltp is None:
                continue
            combined_ltp = ce_ltp + pe_ltp

            # Initialise period end on very first cycle for this basket
            if basket.current_period_end is None:
                basket.current_period_end = get_period_end(now, timings['first_trade_start'])

            # -- PRIORITY 1: Period end -----------------------------------------
            if now >= basket.current_period_end:
                active = basket.get_active_slots()
                if active:
                    self._force_exit_slots(basket, active, ce_ltp, pe_ltp, "PERIOD_END")
                # Advance to next period using arithmetic on current_period_end (robust)
                new_period_end = basket.current_period_end + timedelta(minutes=PERIOD_MINUTES)
                basket.reset_period(new_period_end)
                self.save_state()  # persist reset immediately so crash recovery is clean
                logging.info(f"SMG PERIOD RESET: {level_name} -> next period ends {new_period_end.strftime('%H:%M:%S')}")
                continue  # nothing more to do this cycle for this basket

            active_slots = basket.get_active_slots()

            # -- PRIORITY 2: OI force exit --------------------------------------
            if active_slots and should_oi_force_exit(level_name, OI_SIGNAL_STATE):
                oi_val = OI_CURRENT_DATA['oi_signal_value'] if OI_CURRENT_DATA else 0
                logging.info(f"SMG OI FORCE EXIT: {level_name} signal={OI_SIGNAL_STATE} (value:{oi_val:+,})")
                self._force_exit_slots(basket, active_slots, ce_ltp, pe_ltp, f"OI_{OI_SIGNAL_STATE}")
                active_slots = basket.get_active_slots()

            # -- PRIORITY 3: Basket target / SL --------------------------------
            if active_slots and (ENABLE_BASKET_TARGET or ENABLE_BASKET_SL):
                basket_pnl = basket.get_basket_pnl(combined_ltp)
                if ENABLE_BASKET_TARGET and basket_pnl >= BASKET_TARGET_POINTS:
                    logging.info(f"SMG BASKET TARGET: {level_name} P&L={basket_pnl:.2f} >= {BASKET_TARGET_POINTS}")
                    self._force_exit_slots(basket, active_slots, ce_ltp, pe_ltp, "BASKET_TARGET")
                    active_slots = basket.get_active_slots()
                elif ENABLE_BASKET_SL and basket_pnl <= -BASKET_SL_POINTS:
                    logging.info(f"SMG BASKET SL: {level_name} P&L={basket_pnl:.2f} <= -{BASKET_SL_POINTS}")
                    self._force_exit_slots(basket, active_slots, ce_ltp, pe_ltp, "BASKET_SL")
                    active_slots = basket.get_active_slots()

            # -- PRIORITY 4: Per-slot target / SL ------------------------------
            for slot in list(basket.get_active_slots()):
                if combined_ltp >= slot.target_price:
                    _lat("EXIT_DECISION", 0.0, level=level_name, reason="SLOT_TARGET",
                         ltp_age_ms=_ltp_age_ms, combined_ltp=combined_ltp,
                         slot=slot.slot_num)
                    self._exit_slot(basket, slot, ce_ltp, pe_ltp, "SLOT_TARGET")
                elif ENABLE_SLOT_SL and combined_ltp <= slot.sl_price:
                    _lat("EXIT_DECISION", 0.0, level=level_name, reason="SLOT_SL",
                         ltp_age_ms=_ltp_age_ms, combined_ltp=combined_ltp,
                         slot=slot.slot_num)
                    self._exit_slot(basket, slot, ce_ltp, pe_ltp, "SLOT_SL")

            # -- PRIORITY 5: New slot entries -----------------------------------
            if now >= timings['last_trade_entry']:
                continue

            # Skip if combined premium is too low (worthless straddle)
            if combined_ltp < MIN_ENTRY_PREMIUM:
                continue

            # Check spot proximity eligibility (only trade near current ATM)
            with eligibility_lock:
                can_trade = ACTIVE_STRIKE_PAIRS.get(level_name, {}).get('can_enter_trade', False)
            if not can_trade:
                continue

            # Check OI entry block
            if should_oi_block_entry(level_name, OI_SIGNAL_STATE):
                if random.randint(1, 30) == 1:  # throttle logging
                    oi_val = OI_CURRENT_DATA['oi_signal_value'] if OI_CURRENT_DATA else 0
                    logging.info(f"SMG OI ENTRY BLOCKED: {level_name} signal={OI_SIGNAL_STATE} (value:{oi_val:+,})")
                continue

            # Determine effective VWAP reference
            vwap_ref = basket.vwap_ref if basket.is_vwap_locked else basket.candidate_vwap
            if vwap_ref is None or vwap_ref <= 0:
                continue

            # Price field uses vwap_ref (locked on first entry, consistent for full period)
            if basket.is_vwap_locked:
                entry_pct, target_pct, sl_pct = basket.price_field
            else:
                entry_pct, target_pct, sl_pct = get_price_field(vwap_ref)

            # Collect all unconsumed slots whose LTP threshold is reached.
            # entry_pct may be a list (per-slot distances) or a float (uniform, multiplied by slot_num).
            slots_to_enter = []
            for slot_num in range(1, MAX_SLOTS + 1):
                slot = basket.slots[slot_num]
                if slot.is_consumed:
                    continue
                if isinstance(entry_pct, list):
                    pct = entry_pct[slot_num - 1] if slot_num - 1 < len(entry_pct) else entry_pct[-1]
                    threshold = vwap_ref * (1.0 - pct)
                else:
                    threshold = vwap_ref * (1.0 - slot_num * entry_pct)
                if combined_ltp <= threshold:
                    slots_to_enter.append(slot)

            if not slots_to_enter:
                continue

            # Lock VWAP + price_field + assign basket_id on first entry of this period
            if not basket.is_vwap_locked:
                basket.vwap_ref       = vwap_ref
                basket.price_field    = (entry_pct, target_pct, sl_pct)
                basket.is_vwap_locked = True
                self.basket_counter  += 1
                basket.basket_id      = self.basket_counter  # FIX: store per-basket ID
                logging.info(
                    f"SMG VWAP LOCKED: {level_name} vwap_ref={vwap_ref:.2f} "
                    f"field={entry_pct*100:.0f}% basket_id={basket.basket_id}"
                )

            _lat("ENTRY_DECISION", 0.0, level=level_name,
                 ltp_age_ms=_ltp_age_ms, combined_ltp=combined_ltp,
                 slots=len(slots_to_enter), vwap_ref=vwap_ref)
            self._enter_slots(basket, slots_to_enter, ce_ltp, pe_ltp, combined_ltp)

    # -- Entry helper -----------------------------------------------------------

    def _enter_slots(self, basket, slots, ce_ltp, pe_ltp, combined_ltp):
        """
        Write entry orders for all given slots, then update slot state.
        FIX: basket.basket_id (per-basket, locked counter) used for order IDs.
             Slot state updated ONLY after successful order write.
        """
        _, target_pct, sl_pct = basket.price_field
        # Use the basket's own locked ID, not the engine's live counter
        counter_str = str(basket.basket_id).zfill(4)
        now_entry   = datetime.now(IST)
        orders      = []

        for slot in slots:
            ce_oid = f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_CB"
            pe_oid = f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_PB"
            orders.append((ce_oid, basket.ce_token, 1, max(1.0, ce_ltp + markup), qty, STRATEGY_ID))
            orders.append((pe_oid, basket.pe_token, 1, max(1.0, pe_ltp + markup), qty, STRATEGY_ID))

        # Write orders FIRST; update state only on success
        write_success = write_orders_batch(orders)

        if write_success:
            for slot in slots:
                slot.is_active      = True
                slot.is_consumed    = True
                slot.entry_price    = combined_ltp
                slot.entry_ce_ltp   = ce_ltp
                slot.entry_pe_ltp   = pe_ltp
                slot.entry_time     = now_entry
                slot.basket_counter = basket.basket_id  # matches the order IDs
                # target_pct may be per-slot list or uniform float
                _tgt = (target_pct[slot.slot_num - 1]
                        if isinstance(target_pct, list) and slot.slot_num - 1 < len(target_pct)
                        else (target_pct[-1] if isinstance(target_pct, list) else target_pct))
                slot.target_price   = combined_ltp * (1.0 + _tgt)
                slot.sl_price       = combined_ltp * (1.0 - sl_pct)
                logging.info(
                    f"SMG ENTRY: {basket.level_name} Slot {slot.slot_num} "
                    f"| Combined={combined_ltp:.2f} | Target={slot.target_price:.2f} "
                    f"| SL={slot.sl_price:.2f} | basket_id={basket.basket_id}"
                )
            _queue_slot_event('entry', basket, slots, combined_ltp, ce_ltp, pe_ltp)
            self._state_dirty = True
        else:
            # Orders failed: mark consumed to avoid infinite retry this period,
            # but don't mark active (no real position opened)
            for slot in slots:
                slot.is_consumed = True
            logging.error(
                f"SMG ENTRY FAILED (order write): {basket.level_name} slots "
                f"{[s.slot_num for s in slots]} marked consumed with no active position"
            )
            self._state_dirty = True

    # -- Exit helpers -----------------------------------------------------------

    def _exit_slot(self, basket, slot, ce_ltp, pe_ltp, reason):
        """Exit a single slot. Orders written BEFORE slot state is updated."""
        combined_ltp = ce_ltp + pe_ltp
        pnl          = (combined_ltp - slot.entry_price) * qty
        counter_str  = str(slot.basket_counter).zfill(4)
        orders = [
            (f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_CS",
             basket.ce_token, 2, max(0.05, ce_ltp - markup), qty, STRATEGY_ID),
            (f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_PS",
             basket.pe_token, 2, max(0.05, pe_ltp - markup), qty, STRATEGY_ID),
        ]
        write_success = write_orders_batch(orders)
        if write_success:
            slot.is_active = False  # update state AFTER successful write
        else:
            logging.error(
                f"SMG EXIT FAILED (order write): {basket.level_name} Slot {slot.slot_num} "
                f"- position may still be open at broker. Marking inactive anyway to prevent re-exit."
            )
            slot.is_active = False  # still deactivate to prevent duplicate exit attempts
        logging.info(
            f"SMG EXIT ({reason}): {basket.level_name} Slot {slot.slot_num} "
            f"| Entry={slot.entry_price:.2f} Exit={combined_ltp:.2f} P&L={pnl:.2f}"
        )
        _queue_slot_event('exit', basket, [slot], combined_ltp, ce_ltp, pe_ltp,
                          reason=reason, pnl=pnl)
        self._state_dirty = True

    def _force_exit_slots(self, basket, slots, ce_ltp, pe_ltp, reason):
        """Force exit multiple slots. All orders written in one batch BEFORE state update."""
        if not slots:
            return
        combined_ltp = ce_ltp + pe_ltp
        orders       = []
        for slot in slots:
            counter_str = str(slot.basket_counter).zfill(4)
            orders.append((
                f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_CS",
                basket.ce_token, 2, max(0.05, ce_ltp - markup), qty, STRATEGY_ID,
            ))
            orders.append((
                f"{ORDER_ID_PREFIX}_{counter_str}_{slot.slot_num}_PS",
                basket.pe_token, 2, max(0.05, pe_ltp - markup), qty, STRATEGY_ID,
            ))

        # Write ALL orders FIRST, then update state
        write_success = write_orders_batch(orders)
        for slot in slots:
            slot.is_active = False  # update state AFTER write (whether successful or not)
            pnl = (combined_ltp - slot.entry_price) * qty
            logging.info(
                f"SMG FORCE EXIT ({reason}): {basket.level_name} Slot {slot.slot_num} "
                f"| Entry={slot.entry_price:.2f} Exit={combined_ltp:.2f} P&L={pnl:.2f}"
            )
        if not write_success:
            logging.error(
                f"SMG FORCE EXIT ORDER WRITE FAILED: {basket.level_name} - "
                f"{len(slots)} slots may still be open at broker."
            )
        _queue_slot_event('force_exit', basket, slots, combined_ltp, ce_ltp, pe_ltp,
                          reason=reason)
        self._state_dirty = True

    # -- State persistence ------------------------------------------------------

    def get_state_path(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        return f"{self.state_dir}/{today}_smg_state.json"

    def save_state(self):
        path = self.get_state_path()
        tmp  = path + '.tmp'
        try:
            state = {
                'basket_counter': self.basket_counter,
                'timestamp':      datetime.now(IST).isoformat(),
                'baskets':        {ln: b.to_dict() for ln, b in self.baskets.items()},
            }
            with open(tmp, 'w') as f:
                json.dump(state, f, indent=2, default=str)
                f.flush()
            if os.name == 'nt' and os.path.exists(path):
                os.remove(path)
            os.rename(tmp, path)
        except Exception as e:
            logging.error(f"SMG state save error: {e}")

    def load_state(self):
        path = self.get_state_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, 'r') as f:
                state = json.load(f)
            # FIX: use IST-aware date comparison, not datetime.today() (OS local time)
            saved_dt   = datetime.fromisoformat(state['timestamp'])
            saved_date = saved_dt.astimezone(IST).date() if saved_dt.tzinfo else saved_dt.date()
            today_ist  = datetime.now(IST).date()
            if saved_date != today_ist:
                logging.info("SMG state file is from a previous day - starting fresh.")
                return
            self.basket_counter = state.get('basket_counter', 0)
            for ln, bd in state.get('baskets', {}).items():
                self.baskets[ln] = LevelBasket.from_dict(bd)
            active_count = sum(
                1 for b in self.baskets.values()
                for s in b.slots.values() if s.is_active
            )
            logging.info(f"SMG state loaded: basket_counter={self.basket_counter} active_slots={active_count}")
        except Exception as e:
            logging.error(f"SMG state load error: {e}. Starting fresh.")

# -- EnhancedStrangleFetcher (data thread helper) -------------------------------

class EnhancedStrangleFetcher:
    def __init__(self):
        self.master_df           = self._load_master_file()
        self.last_volume_cache   = {}
        self.session_retry_count = 0

    def _load_master_file(self):
        try:
            if not os.path.exists(BSEFO_MASTER_PATH):
                raise FileNotFoundError(f"Master file not found: {BSEFO_MASTER_PATH}")
            df = pd.read_csv(BSEFO_MASTER_PATH)
            df['OptionType']  = df['OptionType'].replace({3: 'CE', 4: 'PE'})
            df['ExpiryDate']  = pd.to_datetime(df['ContractExpiration']).dt.date
            df['StrikePrice'] = df['StrikePrice'].astype(float)
            return df[df['Name'] == 'SENSEX']
        except Exception as e:
            logging.error(f"Master file load error: {e}")
            raise

    def get_weekly_expiry(self):
        today         = date.today()
        valid_expiries= sorted(e for e in self.master_df['ExpiryDate'].unique() if e >= today)
        return valid_expiries[0] if valid_expiries else None

    def get_index_spot(self, sess=None):
        try:
            spot, _ = get_sensex_spot_bse1(sess or session, keep_trying=False)
            if spot is not None:
                self.session_retry_count = 0
                return spot
        except Exception as e:
            logging.error(f"Spot fetch error: {e}")
        self.session_retry_count += 1
        return None

    def fetch_options_chain_for_active_pairs(self):
        try:
            with eligibility_lock:
                if not ACTIVE_STRIKE_PAIRS:
                    return pd.DataFrame()
                active_copy = {k: v.copy() for k, v in ACTIVE_STRIKE_PAIRS.items()}
            expiry = self.get_weekly_expiry()
            if expiry is None:
                return pd.DataFrame()
            rows = []
            _at_t = time_mod.perf_counter()
            _newest_bar_ts = None
            for level_name, pair in active_copy.items():
                if not pair['is_active']:
                    continue
                ce_data = load_latest_from_db('SENSEX', pair['ce_strike'], 'CE')
                pe_data = load_latest_from_db('SENSEX', pair['pe_strike'], 'PE')
                if ce_data and pe_data:
                    for opt_type, data, strike in [('CE', ce_data, pair['ce_strike']),
                                                   ('PE', pe_data, pair['pe_strike'])]:
                        rows.append({
                            'Level':        level_name,
                            'Instrument':   data['token'],
                            'Type':         opt_type,
                            'Strike':       strike,
                            'Expiry':       expiry,
                            'Open':         data['open'],
                            'High':         data['high'],
                            'Low':          data['low'],
                            'Close':        data['close'],
                            'Volume':       data['volume'],
                            'Timestamp':    data['timestamp'],
                            'IndicatorKey': pair['indicator_key'],
                        })
                    # Track newest bar for staleness measurement
                    if LATENCY_PROFILING:
                        for d in (ce_data, pe_data):
                            ts = d.get('timestamp')
                            if ts is not None and (_newest_bar_ts is None or ts > _newest_bar_ts):
                                _newest_bar_ts = ts
            _at_ms = (time_mod.perf_counter() - _at_t) * 1000
            if not rows:
                return pd.DataFrame()
            if LATENCY_PROFILING:
                _bar_age_ms = -1.0
                if _newest_bar_ts is not None:
                    try:
                        now_ist = datetime.now(IST)
                        _ts = _newest_bar_ts
                        if _ts.tzinfo is None:
                            _ts = IST.localize(_ts)
                        _bar_age_ms = max(0.0, (now_ist - _ts).total_seconds() * 1000)
                    except Exception:
                        pass
                _lat("ALGOTEST_FETCH", _at_ms, levels=len(active_copy), rows=len(rows)//2,
                     bar_age_ms=_bar_age_ms)
            return pd.DataFrame(rows).sort_values(['Level', 'Type', 'Strike'])
        except Exception as e:
            logging.error(f"Options chain fetch error: {e}", exc_info=True)
            return pd.DataFrame()

def analyze_strike_pairs(options_df):
    if options_df is None or options_df.empty:
        return None
    results = []
    for level_name in options_df['Level'].unique():
        ldf = options_df[options_df['Level'] == level_name]
        ce  = ldf[ldf['Type'] == 'CE']
        pe  = ldf[ldf['Type'] == 'PE']
        if ce.empty or pe.empty:
            continue
        cr, pr = ce.iloc[0], pe.iloc[0]
        results.append({
            'Level':          level_name,
            'IndicatorKey':   cr['IndicatorKey'],
            'CE_Strike':      cr['Strike'],
            'PE_Strike':      pr['Strike'],
            'CE_Close':       cr['Close'],  'PE_Close':  pr['Close'],
            'CE_Open':        cr['Open'],   'PE_Open':   pr['Open'],
            'CE_High':        cr['High'],   'PE_High':   pr['High'],
            'CE_Low':         cr['Low'],    'PE_Low':    pr['Low'],
            'Combined_Open':  cr['Open']  + pr['Open'],
            'Combined_High':  cr['High']  + pr['High'],
            'Combined_Low':   cr['Low']   + pr['Low'],
            'Combined_Close': cr['Close'] + pr['Close'],
            'Combined_Volume':cr['Volume']+ pr['Volume'],
            'CE_Token':       cr['Instrument'],
            'PE_Token':       pr['Instrument'],
            'Expiry':         cr['Expiry'],
            'Timestamp':      cr['Timestamp'],
        })
    return pd.DataFrame(results) if results else None

# -- Zerodha background LTP refresh --------------------------------------------

def _zerodha_refresh_loop():
    """Refresh Zerodha LTP cache every 500ms in a background thread.
    Keeps main loop unblocked: process_cycle() now reads an always-fresh in-memory cache.
    ZerodhaMongoReader.lock already protects all cache reads/writes - no new races.
    """
    while True:
        try:
            if zerodha_reader is not None:
                zerodha_reader.refresh_cache()
        except Exception as e:
            logging.error(f"Zerodha refresh thread error: {e}")
        time_mod.sleep(0.6)  # 0.6s > refresh_interval(0.5s): avoids throttle edge case on Windows


# -- Data thread ----------------------------------------------------------------

def fetch_and_process_data(fetcher, data_queue):
    global data_thread_alive
    data_thread_alive.set()
    try:
        while True:
            timings = get_market_timings()
            if datetime.now(IST) >= timings['kill_time']:
                break
            start = time_mod.time()
            # Spot: try local CSV first (~0.1ms), fall back to Algotest HTTP (~91ms) if unavailable.
            current_spot = get_sensex_spot_from_csv()
            if current_spot is None:
                current_spot = fetcher.get_index_spot(session)
            if current_spot:
                update_trade_eligibility_by_spot(current_spot)
            options_df = fetcher.fetch_options_chain_for_active_pairs()
            if options_df is not None and not options_df.empty:
                try:
                    sp_data = analyze_strike_pairs(options_df.copy())
                    if sp_data is not None and not sp_data.empty:
                        # Non-blocking put: replace oldest item if queue is full
                        try:
                            data_queue.put_nowait((options_df, sp_data))
                        except queue.Full:
                            try:
                                data_queue.get_nowait()
                            except queue.Empty:
                                pass
                            data_queue.put_nowait((options_df, sp_data))
                        update_candles_by_indicator_key(sp_data.copy(), fetcher)
                        save_candles(fetcher.last_volume_cache)
                except Exception as e:
                    logging.error(f"Data thread analysis error: {e}")
            elapsed = time_mod.time() - start
            time_mod.sleep(max(0, 1.0 - elapsed))
            data_thread_alive.set()
    except Exception as e:
        logging.error(f"Data thread error: {e}")
    finally:
        data_thread_alive.clear()
        logging.info("Data thread terminated")

# -- Main ----------------------------------------------------------------------

def main():
    global session, zerodha_reader, eligibility_lock, eligibility_queue, eligibility_thread_alive
    global OI_SIGNAL_STATE, LAST_OI_CHECK_TIME, OI_CURRENT_DATA

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('smg_trading_log.log'),
            logging.StreamHandler(),
        ],
    )

    if LATENCY_PROFILING:
        global latency_logger
        latency_logger = logging.getLogger('smg_latency')
        latency_logger.setLevel(logging.INFO)
        lh = logging.FileHandler('smg_latency.log')
        lh.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        latency_logger.addHandler(lh)
        latency_logger.propagate = False
        logging.info("Latency profiling ENABLED -> smg_latency.log")

    # Login to Algotest
    PHONE    = "+919711801082"
    PASSWORD = "password1759"
    session  = requests.Session()
    resp     = session.post(
        "https://api.algotest.in/login",
        json={"phoneNumber": PHONE, "password": PASSWORD},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    logging.info("Algotest login successful")

    # Zerodha MongoDB reader
    if ZERODHA_ENABLED:
        try:
            zerodha_reader = ZerodhaMongoReader(ZERODHA_MONGO_URI, ZERODHA_MONGO_DB)
            if not zerodha_reader.refresh_cache(force=True):
                raise RuntimeError("Zerodha cache empty on startup")
            stats = zerodha_reader.get_cache_stats()
            logging.info(f"Zerodha reader: {stats['cache_size']} instruments loaded")
        except Exception as e:
            logging.error(f"Zerodha init failed: {e}")
            raise SystemExit("Zerodha integration required")

    # ATM levels
    initialize_fixed_atm_levels()

    # Engine + fetcher + candles
    engine = SlicedMangoEngine()
    fetcher = EnhancedStrangleFetcher()
    restored_cache = load_candles()
    if restored_cache:
        fetcher.last_volume_cache = restored_cache
        logging.info(f"Volume cache restored: {len(restored_cache)} entries")

    # Thread safety primitives
    eligibility_lock         = threading.RLock()
    eligibility_queue        = queue.Queue(maxsize=10)
    eligibility_thread_alive = threading.Event()

    # Baskets (after ATM and eligibility_lock are ready)
    engine.initialize_baskets()

    # Data thread
    data_queue  = queue.Queue(maxsize=5)
    data_thread = threading.Thread(
        target=fetch_and_process_data, args=(fetcher, data_queue), daemon=True
    )
    data_thread.start()

    # Eligibility thread
    elig_thread = threading.Thread(target=eligibility_background_processor, daemon=True)
    elig_thread.start()

    # Zerodha LTP refresh background thread (replaces blocking call in main loop)
    zr_thread = threading.Thread(target=_zerodha_refresh_loop, daemon=True)
    zr_thread.start()
    logging.info("Zerodha LTP refresh background thread started (500ms interval).")

    # Slot event CSV writer background thread (GUI reads smg_slots_YYYYMMDD.csv)
    slot_writer_thread = threading.Thread(target=_slot_event_writer_loop, daemon=True)
    slot_writer_thread.start()
    logging.info("Slot event CSV writer background thread started.")

    logging.info("SlicedMango engine started. Entering main loop.")

    force_exit_triggered = False  # guard against re-triggering 15:20 force exit every cycle

    while True:
        try:
            timings = get_market_timings()
            now     = datetime.now(IST)

            # -- Kill time ------------------------------------------------------
            if now >= timings['kill_time']:
                logging.info("Kill time reached - force-exiting all active slots and stopping")
                if ZERODHA_ENABLED and zerodha_reader:
                    zerodha_reader.refresh_cache(force=True)  # force fresh data at exit
                for level_name, basket in engine.baskets.items():
                    active = basket.get_active_slots()
                    if active:
                        ce_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.ce_strike, "CE") if zerodha_reader else None
                        pe_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.pe_strike, "PE") if zerodha_reader else None
                        # FIX: use 'is not None' not truthiness (LTP could legitimately be 0.0)
                        if ce_ltp is not None and pe_ltp is not None:
                            engine._force_exit_slots(basket, active, ce_ltp, pe_ltp, "KILL_TIME")
                        else:
                            logging.error(
                                f"KILL_TIME: LTP unavailable for {level_name} "
                                f"(CE={ce_ltp}, PE={pe_ltp}) - {len(active)} slots NOT exited"
                            )
                save_candles(fetcher.last_volume_cache)
                engine.save_state()
                break

            # -- Force exit time (15:20) ----------------------------------------
            if now >= timings['force_exit_time'] and not force_exit_triggered:
                force_exit_triggered = True
                logging.info("Force exit time (15:20) - closing all active slots")
                if ZERODHA_ENABLED and zerodha_reader:
                    zerodha_reader.refresh_cache(force=True)  # force fresh data at exit
                for level_name, basket in engine.baskets.items():
                    active = basket.get_active_slots()
                    if active:
                        ce_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.ce_strike, "CE") if zerodha_reader else None
                        pe_ltp = zerodha_reader.get_ltp_by_strike("SENSEX", basket.pe_strike, "PE") if zerodha_reader else None
                        if ce_ltp is not None and pe_ltp is not None:
                            engine._force_exit_slots(basket, active, ce_ltp, pe_ltp, "FORCE_EXIT_TIME")
                        else:
                            logging.error(
                                f"FORCE_EXIT: LTP unavailable for {level_name} - {len(active)} slots NOT exited"
                            )
                engine.save_state()
                engine._state_dirty = False
                # continue looping (kill_time at 15:20:30 will break the loop)

            # -- Try to get fresh options / candle data (non-blocking) ----------
            # Spot fetch and eligibility update now happen in data thread.
            try:
                options_df, sp_data = data_queue.get_nowait()
                engine.update_tokens_from_options(options_df)
            except queue.Empty:
                pass

            # Zerodha LTP refresh now runs in _zerodha_refresh_loop() background thread.
            # No blocking call here - main loop reads the always-fresh in-memory cache.

            # -- Candidate VWAP refresh -----------------------------------------
            engine.update_candidate_vwaps()

            # -- Periodic OI signal check (every 2 minutes) --------------------
            if ENABLE_OI_EXIT:
                time_since_oi = (
                    (now - LAST_OI_CHECK_TIME).total_seconds()
                    if LAST_OI_CHECK_TIME is not None else 999
                )
                if time_since_oi >= 120:
                    try:
                        check_oi_signal_and_update()
                    except Exception as e:
                        logging.error(f"OI signal check error: {e}", exc_info=True)

            # -- Thread health check --------------------------------------------
            if not data_thread.is_alive():
                logging.error("DATA THREAD DIED - no new candle data will arrive!")
            if not elig_thread.is_alive():
                logging.error("ELIGIBILITY THREAD DIED - trade eligibility frozen!")
            if not zr_thread.is_alive():
                logging.error("ZERODHA REFRESH THREAD DIED - LTP cache will go stale!")

            # -- Main per-level logic -------------------------------------------
            if now >= timings['first_trade_start']:
                _t = time_mod.perf_counter()
                engine.process_cycle()
                _lat("PROCESS_CYCLE", (time_mod.perf_counter() - _t) * 1000)
                # Coalesced state save: trade methods set _state_dirty=True instead of
                # calling save_state() immediately. We save once per cycle here.
                if engine._state_dirty:
                    engine.save_state()
                    engine._state_dirty = False

            # -- Occasional status log ------------------------------------------
            if random.randint(1, 30) == 1:
                active_total = sum(len(b.get_active_slots()) for b in engine.baskets.values())
                oi_str = f"OI={OI_SIGNAL_STATE}" if OI_SIGNAL_STATE else "OI=NEUTRAL"
                logging.info(
                    f"SMG STATUS: active_slots={active_total} "
                    f"basket_counter={engine.basket_counter} {oi_str}"
                )

            time_mod.sleep(1.0)

        except Exception as e:
            logging.error(f"Main loop error: {e}", exc_info=True)
            engine.save_state()
            engine._state_dirty = False
            time_mod.sleep(1.0)


if __name__ == "__main__":
    while True:
        timings = get_market_timings()
        now     = datetime.now(IST)
        if now >= timings['kill_time']:
            print("Kill time reached. Program will not start.")
            break
        if now >= timings['market_open']:
            print(f"Starting SlicedMango at {now.strftime('%H:%M:%S')} IST")
            main()
            post_exit = datetime.now(IST)
            if post_exit >= get_market_timings()['kill_time']:
                print("Kill time reached. Terminating.")
                break
            else:
                print("Main exited unexpectedly, restarting...")
        else:
            print(f"Market not open. Current: {now.strftime('%H:%M:%S')} IST")
        time_mod.sleep(1)
