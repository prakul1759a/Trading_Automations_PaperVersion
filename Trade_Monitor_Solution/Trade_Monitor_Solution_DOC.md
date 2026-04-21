# Trade Monitor Solution

Efficient multi-session trade log viewer. Parses the trading engine's log file into SQLite once, then allows instant navigation across months of trade history.

---

## Problem Solved

The Trade GUI (`gui_try.py` in each paper trading folder) re-reads the entire `trading_log.log` on every refresh. After a few days of continuous trading this file grows to several GB. Switching dates or refreshing takes minutes or causes the UI to hang.

This solution parses the log once into a SQLite cache, loads it into memory on startup, and makes all date queries instant regardless of how many months of data are in the log.

---

## Files

| File | Purpose |
|------|---------|
| `Monitor_log_parser_service.py` | Parses `trading_log.log` into `trading_data.db`. On first run: full parse. On subsequent runs: tail-watches for new entries and inserts incrementally. |
| `Monitor_enhanced_Trade_gui_with_Target_Integration_v2_DB.py` | Tkinter GUI reading from the SQLite DB. Date navigation, trade grid, P&L display, status panel. |
| `trade_data_cache.py` | In-memory cache layer so the GUI does not re-query the DB on every refresh cycle. |

---

## How to Use

**First run (building the cache):**
```
python Monitor_log_parser_service.py
```
Point it at the `trading_log.log` file from the paper trading engine. Full parse takes a few minutes depending on log size. Writes `trading_data.db` to the same directory.

**Subsequent runs:**
```
python Monitor_enhanced_Trade_gui_with_Target_Integration_v2_DB.py
```
Loads the DB into memory instantly. Use the date navigation bar to jump to any session.

The parser service can also run alongside the trading engine during market hours — it tail-watches the log and inserts new events incrementally so the GUI stays current.

---

## Features

- Date navigation bar (previous/next day, date picker)
- Trade grid showing all entries/exits for the selected date
- P&L summary per session and cumulative
- Target integration — shows whether target was hit and at what time
- Status panel showing engine state for the selected session
- Screenshot capture (requires `pyautogui`)

---

## Configuration

At the top of each file:

```
LOG_FILE_PATH         path to trading_log.log from the paper trading engine
DB_PATH               where to write/read trading_data.db
```

---

## Dependencies

`tkinter` (stdlib), `sqlite3` (stdlib), `pyautogui` (optional, for screenshot only)
