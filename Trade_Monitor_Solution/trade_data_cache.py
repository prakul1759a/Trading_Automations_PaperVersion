"""
Advanced Trade Data Cache Service
Optimizes loading of trading data from your existing files
Creates an indexed cache for instant access to trade data
"""

import sqlite3
import json
import os
import csv
from datetime import datetime
from pathlib import Path
import glob


class TradeDataCache:
    def __init__(self, cache_db="trade_cache.db", log_file="trading_log.log", trades_dir="."):
        self.cache_db = cache_db
        self.log_file = log_file
        self.trades_dir = trades_dir
        self.conn = None
        
    def init_cache(self):
        """Initialize cache database"""
        self.conn = sqlite3.connect(self.cache_db, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Trades cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades_cache (
                trade_id TEXT,
                date TEXT NOT NULL,
                entry_time TEXT,
                strike TEXT,
                entry_spot REAL,
                signal_entry TEXT,
                entry_price REAL,
                exit_time TEXT,
                exit_spot REAL,
                signal_exit TEXT,
                exit_price REAL,
                current_price REAL,
                qty INTEGER,
                mtm REAL,
                source TEXT,
                PRIMARY KEY (trade_id, date)
            )
        ''')
        
        # Available dates cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache_metadata (
                cache_key TEXT PRIMARY KEY,
                value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for fast queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_date ON trades_cache(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_trade_id ON trades_cache(trade_id)')
        
        self.conn.commit()
        print(f"Trade cache initialized: {self.cache_db}")
    
    def load_csv_trades(self, csv_file, date):
        """Load trades from CSV file (your format)"""
        trades = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Parse strike from string format "[83800, 83800]"
                    strike_str = row.get('Strike', '').strip()
                    if strike_str.startswith('['):
                        strike = strike_str.split(',')[0].replace('[', '').strip()
                    else:
                        strike = strike_str
                    
                    # Parse signal entry
                    signal_entry = "Short" if row.get('Signal Entry', '').strip() == "Short" else "Long"
                    
                    # Parse MTM
                    mtm_str = row.get('MTM', '0').replace('+', '').strip()
                    try:
                        mtm = float(mtm_str)
                    except:
                        mtm = 0.0
                    
                    trade = {
                        'trade_id': row.get('Trade ID', ''),
                        'date': date,
                        'entry_time': row.get('Entry Time', ''),
                        'strike': strike,
                        'entry_spot': self._safe_float(row.get('Entry Spot', '')),
                        'signal_entry': signal_entry,
                        'entry_price': self._safe_float(row.get('Entry Price', '')),
                        'exit_time': row.get('Exit Time', '') or None,
                        'exit_spot': self._safe_float(row.get('Exit Spot', '')) if row.get('Exit Spot', '') else None,
                        'signal_exit': row.get('Signal Exit', '') if row.get('Signal Exit', '') else None,
                        'exit_price': self._safe_float(row.get('Exit Price', '')) if row.get('Exit Price', '') else None,
                        'current_price': self._safe_float(row.get('Current Price', '')),
                        'qty': int(row.get('QTY', '20')),
                        'mtm': mtm,
                        'source': 'CSV'
                    }
                    trades.append(trade)
        except Exception as e:
            print(f"Error loading CSV {csv_file}: {e}")
        
        return trades
    
    def _safe_float(self, value):
        """Safely convert value to float"""
        try:
            return float(str(value).strip())
        except:
            return 0.0
    
    def build_cache_from_csv(self):
        """Build cache from CSV files (your actual data source)"""
        print("Building trade cache from CSV files...")
        
        self.init_cache()
        cursor = self.conn.cursor()
        
        # Find all trades_YYYY-MM-DD.csv files
        csv_files = glob.glob(os.path.join(self.trades_dir, "**/trades_*.csv"), recursive=True)
        csv_files += glob.glob(os.path.join(self.trades_dir, "trades_*.csv"))
        
        dates_set = set()
        total_trades = 0
        
        for csv_file in sorted(csv_files):
            try:
                # Extract date from filename (trades_2026-02-12.csv)
                filename = os.path.basename(csv_file)
                if filename.startswith('trades_') and filename.endswith('.csv'):
                    date_str = filename.replace('trades_', '').replace('.csv', '')
                    
                    print(f"  Processing: {filename}")
                    trades = self.load_csv_trades(csv_file, date_str)
                    
                    # Insert trades into cache
                    for trade in trades:
                        cursor.execute('''
                            INSERT OR REPLACE INTO trades_cache
                            (trade_id, date, entry_time, strike, entry_spot, signal_entry,
                             entry_price, exit_time, exit_spot, signal_exit, exit_price,
                             current_price, qty, mtm, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            trade['trade_id'], trade['date'], trade['entry_time'],
                            trade['strike'], trade['entry_spot'], trade['signal_entry'],
                            trade['entry_price'], trade['exit_time'], trade['exit_spot'],
                            trade['signal_exit'], trade['exit_price'], trade['current_price'],
                            trade['qty'], trade['mtm'], trade['source']
                        ))
                    
                    total_trades += len(trades)
                    dates_set.add(date_str)
                    print(f"    → Loaded {len(trades)} trades")
            
            except Exception as e:
                print(f"  ✗ Error processing {csv_file}: {e}")
        
        self.conn.commit()
        
        print(f"\n✓ Cache build complete!")
        print(f"  - {total_trades} total trades loaded")
        print(f"  - {len(dates_set)} trading dates cached")
        
        return total_trades
    
    def get_trades_for_date(self, date):
        """Get all trades for a date"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM trades_cache WHERE date = ? ORDER BY entry_time
        ''', (date,))
        return cursor.fetchall()
    
    def get_available_dates(self):
        """Get all available trading dates"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT date FROM trades_cache ORDER BY date DESC')
        return [row[0] for row in cursor.fetchall()]
    
    def get_date_summary(self, date):
        """Get summary for a date"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN exit_price IS NOT NULL THEN 1 ELSE 0 END) as closed_trades,
                SUM(CASE WHEN exit_price IS NULL THEN 1 ELSE 0 END) as open_trades,
                SUM(mtm) as total_mtm
            FROM trades_cache WHERE date = ?
        ''', (date,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {'total_trades': 0, 'closed_trades': 0, 'open_trades': 0, 'total_mtm': 0}
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()


def main():
    """Build cache from your existing CSV files"""
    
    # Adjust path as needed - this should find your CSV files
    base_path = "."
    
    cache = TradeDataCache(cache_db="trade_cache.db", trades_dir=base_path)
    
    try:
        total = cache.build_cache_from_csv()
        
        if total > 0:
            dates = cache.get_available_dates()
            print(f"\nAvailable dates: {len(dates)}")
            if dates:
                print(f"Recent dates: {dates[:5]}")
                
                # Show summary for latest date
                latest_date = dates[0]
                summary = cache.get_date_summary(latest_date)
                print(f"\nSummary for {latest_date}:")
                print(f"  Total trades: {summary['total_trades']}")
                print(f"  Closed: {summary['closed_trades']}, Open: {summary['open_trades']}")
                print(f"  Total MTM: {summary['total_mtm']:.2f}")
        
        cache.close()
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
