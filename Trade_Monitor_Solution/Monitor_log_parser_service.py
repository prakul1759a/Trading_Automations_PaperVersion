"""
Log Parser Service - Converts large trading log files to SQLite database for efficient querying
This service runs once to parse logs and can watch for updates without blocking the dashboard
"""

import sqlite3
import json
import re
import ast
import os
from datetime import datetime
from pathlib import Path
import threading
import time


class LogParserService:
    def __init__(self, log_file="trading_log.log", db_file="trading_data.db"):
        self.log_file = log_file
        self.db_file = db_file
        self.conn = None
        self.last_position = 0  # Track file position for incremental updates
        
    def init_database(self):
        """Create database schema"""
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Main trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                entry_time TEXT,
                strike INTEGER,
                entry_spot REAL,
                signal_entry INTEGER,
                entry_price REAL,
                exit_time TEXT,
                exit_spot REAL,
                signal_exit TEXT,
                exit_price REAL,
                qty INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Price updates table (for current prices)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT NOT NULL,
                date TEXT NOT NULL,
                timestamp TEXT,
                strangle_price REAL,
                spot REAL,
                strikes INTEGER,
                FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
            )
        ''')
        
        # Available dates table (for quick listing)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS available_dates (
                date TEXT PRIMARY KEY,
                trade_count INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Parser state table (for incremental updates)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parser_state (
                id INTEGER PRIMARY KEY,
                log_file TEXT,
                last_position INTEGER,
                last_parsed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for fast queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_id ON trades(trade_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_updates_date ON price_updates(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_updates_trade_id ON price_updates(trade_id)')
        
        self.conn.commit()
        print(f"Database initialized: {self.db_file}")
    
    def parse_log_data(self, log_data):
        """Parse JSON/Python dict from log data"""
        log_data = log_data.strip()
        log_data = log_data.replace("'", '"')
        log_data = re.sub(r'Timestamp\((["\'])(.*?)\1\)', r'\2', log_data)
        
        try:
            return json.loads(log_data)
        except json.JSONDecodeError:
            return ast.literal_eval(log_data)
    
    def _safe_value(self, value):
        """Convert any value to a safe scalar type for database"""
        if value is None:
            return None
        if isinstance(value, list):
            # If it's a list, try to get first element or convert to string
            return value[0] if value else None
        if isinstance(value, dict):
            # If it's a dict, return None
            return None
        return value
    
    def _safe_float(self, value):
        """Safely convert to float"""
        try:
            v = self._safe_value(value)
            return float(v) if v is not None else None
        except:
            return None
    
    def _safe_int(self, value):
        """Safely convert to int"""
        try:
            v = self._safe_value(value)
            return int(v) if v is not None else None
        except:
            return None
    
    def parse_full_log(self, progress_callback=None):
        """Parse entire log file and populate database"""
        if not os.path.exists(self.log_file):
            print(f"Log file not found: {self.log_file}")
            return 0
        
        print(f"Parsing log file: {self.log_file}")
        self.init_database()
        
        trades_dict = {}
        dates_set = set()
        price_updates = []
        lines_processed = 0
        file_size = os.path.getsize(self.log_file)
        
        cursor = self.conn.cursor()
        
        try:
            with open(self.log_file, "r") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        parts = line.split(" - ", 4)
                        if len(parts) <= 3:
                            continue
                        
                        timestamp, _, event_type, log_data = parts
                        timestamp_date = timestamp.split()[0]
                        dates_set.add(timestamp_date)
                        
                        event_type = event_type.strip()
                        entry = self.parse_log_data(log_data)
                        
                        # Handle ENTRY events
                        if event_type == "ENTRY":
                            trade_id = entry.get('trade_id')
                            if trade_id:
                                signal_val = entry.get('signal', {})
                                if isinstance(signal_val, dict):
                                    signal_entry = signal_val.get('signal')
                                else:
                                    signal_entry = signal_val
                                
                                trades_dict[trade_id] = {
                                    'trade_id': trade_id,
                                    'date': timestamp_date,
                                    'entry_time': timestamp.split(',')[0],
                                    'strike': self._safe_int(entry.get('strike')),
                                    'entry_spot': self._safe_float(entry.get('spot')),
                                    'signal_entry': self._safe_value(signal_entry),
                                    'entry_price': self._safe_float(entry.get('call_ltp', 0)) + self._safe_float(entry.get('put_ltp', 0)) if self._safe_float(entry.get('call_ltp', 0)) and self._safe_float(entry.get('put_ltp', 0)) else None,
                                    'exit_time': None,
                                    'exit_spot': None,
                                    'signal_exit': None,
                                    'exit_price': None,
                                    'qty': 20
                                }
                        
                        # Handle EXIT events
                        elif event_type == "EXIT":
                            trade_id = entry.get('trade_id')
                            if trade_id in trades_dict:
                                signal_value = entry.get('signal', {})
                                if isinstance(signal_value, dict):
                                    signal_value = signal_value.get('signal')
                                exit_signal = 'target' if signal_value == "TARGET" else self._safe_value(signal_value)
                                trades_dict[trade_id].update({
                                    'exit_time': timestamp.split(',')[0],
                                    'exit_spot': self._safe_float(entry.get('exit_spot')),
                                    'signal_exit': exit_signal,
                                    'exit_price': self._safe_float(entry.get('call_ltp', 0)) + self._safe_float(entry.get('put_ltp', 0)) if self._safe_float(entry.get('call_ltp', 0)) and self._safe_float(entry.get('put_ltp', 0)) else None
                                })
                        
                        # Handle TARGET_EXIT events
                        elif event_type == "TARGET_EXIT":
                            trade_id = entry.get('trade_id')
                            if trade_id in trades_dict:
                                trades_dict[trade_id].update({
                                    'exit_time': timestamp.split(',')[0],
                                    'exit_spot': self._safe_float(entry.get('exit_spot')),
                                    'signal_exit': 'target',
                                    'exit_price': self._safe_float(entry.get('call_ltp', 0)) + self._safe_float(entry.get('put_ltp', 0)) if self._safe_float(entry.get('call_ltp', 0)) and self._safe_float(entry.get('put_ltp', 0)) else None
                                })
                        
                        # Handle PRICE_UPDATE events
                        elif event_type == "PRICE_UPDATE":
                            trade_id = entry.get('trade_id')
                            if trade_id is not None:
                                price_updates.append({
                                    'trade_id': trade_id,
                                    'date': timestamp_date,
                                    'timestamp': timestamp,
                                    'strangle_price': self._safe_float(entry.get('strangle_price')),
                                    'spot': self._safe_float(entry.get('spot')),
                                    'strikes': self._safe_int(entry.get('strikes'))
                                })
                        
                        lines_processed += 1
                        
                        # Progress callback every 10000 lines
                        if progress_callback and lines_processed % 10000 == 0:
                            progress = int((f.tell() / file_size) * 100)
                            progress_callback(lines_processed, progress)
                    
                    except Exception as e:
                        # Skip problematic lines
                        continue
        
        except Exception as e:
            print(f"Error reading log file: {e}")
            return 0
        
        # Insert trades into database
        print(f"Inserting {len(trades_dict)} trades into database...")
        for trade in trades_dict.values():
            cursor.execute('''
                INSERT OR REPLACE INTO trades 
                (trade_id, date, entry_time, strike, entry_spot, signal_entry, 
                 entry_price, exit_time, exit_spot, signal_exit, exit_price, qty)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade['trade_id'], trade['date'], trade['entry_time'],
                trade['strike'], trade['entry_spot'], trade['signal_entry'],
                trade['entry_price'], trade['exit_time'], trade['exit_spot'],
                trade['signal_exit'], trade['exit_price'], trade['qty']
            ))
        
        # Insert price updates
        print(f"Inserting {len(price_updates)} price updates into database...")
        for update in price_updates:
            cursor.execute('''
                INSERT INTO price_updates (trade_id, date, timestamp, strangle_price, spot, strikes)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (update['trade_id'], update['date'], update['timestamp'],
                  update['strangle_price'], update['spot'], update['strikes']))
        
        # Update available dates
        print(f"Updating {len(dates_set)} available dates...")
        for date in dates_set:
            date_trade_count = sum(1 for t in trades_dict.values() if t['date'] == date)
            cursor.execute('''
                INSERT OR REPLACE INTO available_dates (date, trade_count)
                VALUES (?, ?)
            ''', (date, date_trade_count))
        
        self.conn.commit()
        print(f"✓ Parsing complete! Processed {lines_processed} log lines")
        print(f"  - {len(trades_dict)} unique trades")
        print(f"  - {len(dates_set)} trading dates")
        
        return len(trades_dict)
    
    def get_trades_for_date(self, date):
        """Get all trades for a specific date"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM trades WHERE date = ? ORDER BY entry_time
        ''', (date,))
        return cursor.fetchall()
    
    def get_available_dates(self):
        """Get all available trading dates"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT date FROM available_dates ORDER BY date DESC')
        return [row[0] for row in cursor.fetchall()]
    
    def get_latest_prices(self, date):
        """Get latest prices for all trades on a date"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT trade_id, strangle_price, spot, strikes FROM price_updates
            WHERE date = ?
            ORDER BY timestamp DESC
        ''', (date,))
        
        prices = {}
        for row in cursor.fetchall():
            trade_id = row[0]
            if trade_id not in prices:  # Keep latest (first) entry
                prices[trade_id] = {
                    'price': row[1],
                    'spot': row[2],
                    'strikes': row[3]
                }
        return prices
    
    def get_current_state(self):
        """Get current market state (latest prices and spot)"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT spot, strikes FROM price_updates
            ORDER BY timestamp DESC LIMIT 1
        ''')
        row = cursor.fetchone()
        if row:
            return {'spot': row[0], 'strike': row[1]}
        return {'spot': 0.0, 'strike': 0}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """Main entry point for parsing logs"""
    parser = LogParserService()
    
    def progress(lines, percent):
        print(f"  Progress: {lines} lines | {percent}% complete")
    
    try:
        trades_count = parser.parse_full_log(progress_callback=progress)
        
        if trades_count > 0:
            print("\n✓ Database created successfully!")
            
            # Show sample data
            dates = parser.get_available_dates()
            print(f"\nAvailable dates: {len(dates)}")
            if dates:
                print(f"Recent dates: {dates[:5]}")
                
                # Show sample trades from latest date
                latest_date = dates[0]
                trades = parser.get_trades_for_date(latest_date)
                print(f"\nSample trades from {latest_date}: {len(trades)} trades")
        
        parser.close()
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
