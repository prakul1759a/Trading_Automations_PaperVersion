import os
import pickle
import pandas as pd
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

class EnhancedCandleGUI:
    def __init__(self, master):
        self.master = master
        self.master.title("Enhanced Candle Data Viewer - Strike Pairs")
        
        # Create main frame with date selector
        main_frame = ttk.Frame(master)
        main_frame.pack(fill='both', expand=True)
        
        # Date selection frame
        date_frame = ttk.Frame(main_frame)
        date_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(date_frame, text="Select Date:").pack(side='left', padx=5)
        
        self.date_var = tk.StringVar(value=datetime.today().strftime('%Y-%m-%d'))
        self.date_entry = ttk.Entry(date_frame, textvariable=self.date_var, width=12)
        self.date_entry.pack(side='left', padx=5)
        
        ttk.Button(date_frame, text="Load Date", command=self.load_selected_date).pack(side='left', padx=5)
        ttk.Button(date_frame, text="Previous Day", command=self.load_previous_day).pack(side='left', padx=5)
        ttk.Button(date_frame, text="Next Day", command=self.load_next_day).pack(side='left', padx=5)
        ttk.Button(date_frame, text="Today", command=self.load_today).pack(side='left', padx=5)
        
        # Available dates display
        self.available_dates_var = tk.StringVar(value="Available dates: Loading...")
        ttk.Label(date_frame, textvariable=self.available_dates_var).pack(side='left', padx=20)
        
        # Notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill='both', expand=True)
        
        # Initialize variables
        self.current_date = self.date_var.get()
        self.data_dir = f"candle_data/{self.current_date}"
        self.input_file = f"{self.data_dir}/strike_pair_candles.pkl"
        self.last_mod_time = None
        self.strike_pair_tabs = {}
        self.available_dates = []
        
        self.setup_gui()
        self.scan_available_dates()
        self.update_gui()
        
    def get_candles_with_fallback(self, date_data):
        """Get candles with fallback to available dates"""
        date_key = self.match_date_key(date_data, self.current_date)
        
        if not date_key:
            all_dates = sorted(date_data.keys(), reverse=True)
            date_key = all_dates[0] if all_dates else None
            
        if date_key:
            return date_data[date_key].get('1min', {})
        return {}

    def match_date_key(self, date_data, target_date):
        """Handle possible date format variations"""
        target_variants = {
            target_date,
            target_date.replace("-", ""),
            datetime.strptime(target_date, "%Y-%m-%d").strftime("%Y%m%d")
        }
        
        for key in date_data.keys():
            if key in target_variants:
                return key
        return None
    
    def setup_gui(self):
        """Initial setup for notebook and tabs"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

    def safe_pickle_load(self, filepath):
        """Safely load pickle data with error handling for truncated files"""
        try:
            # Read file into memory first, then close immediately to minimize lock time
            with open(filepath, 'rb') as f:
                file_content = f.read()
            # File is now closed - deserialize from memory
            loaded_data = pickle.loads(file_content)

            # Handle new format with volume cache (backward compatible)
            if isinstance(loaded_data, dict) and '_volume_cache_' in loaded_data:
                # New format: extract just the candles
                data = loaded_data['candles']
            else:
                # Old format: already just candles
                data = loaded_data

            return data, None
        except (pickle.UnpicklingError, EOFError) as e:
            if "truncated" in str(e).lower() or "eof" in str(e).lower():
                # File is truncated, try to recover what we can
                print(f"Pickle file appears truncated: {e}")
                print("Attempting to recover partial data...")
                
                try:
                    # Try to read partial data - read into memory first
                    partial_data = {}
                    with open(filepath, 'rb') as f:
                        file_content = f.read()
                    # File closed - now try to recover from memory
                    import io
                    buffer = io.BytesIO(file_content)
                    while True:
                        try:
                            chunk = pickle.load(buffer)
                            if isinstance(chunk, dict):
                                # Handle new format during recovery too
                                if '_volume_cache_' in chunk:
                                    partial_data.update(chunk['candles'])
                                else:
                                    partial_data.update(chunk)
                        except (pickle.UnpicklingError, EOFError):
                            break

                    if partial_data:
                        print(f"Recovered {len(partial_data)} items from truncated file")
                        return partial_data, "Partial data recovered from truncated file"
                    else:
                        return {}, "File is corrupted, no data recovered"
                except Exception as recovery_error:
                    print(f"Failed to recover data: {recovery_error}")
                    return {}, f"File corrupted: {e}"
            else:
                return {}, f"Pickle error: {e}"
        except Exception as e:
            return {}, f"Unexpected error: {e}"
    
    def load_and_process_data(self):
        """Load and process the enhanced strike pair candle data with error recovery"""
        try:
            if not os.path.exists(self.input_file):
                print(f"Candle file not found: {self.input_file}")
                return None
                
            current_mod_time = os.path.getmtime(self.input_file)
            if current_mod_time == self.last_mod_time:
                return None
            
            self.last_mod_time = current_mod_time
            
            # Use safe pickle loading
            data, error_msg = self.safe_pickle_load(self.input_file)
            
            if error_msg:
                # Update window title to show error status
                self.master.title(f"Enhanced Candle Viewer - {self.current_date} - {error_msg}")
            
            if not data:
                return None
            
            processed = {}
            # Data structure: {indicator_key: {date: {'1min': {timestamp: candle_data}}}}
            for indicator_key, date_data in data.items():
                candles = self.get_candles_with_fallback(date_data)
                df = self.create_dataframe(candles)
                if not df.empty:
                    df['rsi'] = self.get_rsi_from_candles(candles)

                    # ensure chronological for stateful calc (your candles are already sorted, but just in case)
                    df.sort_values('timestamp', inplace=True)

                    state = 0   # start flat (0)
                    sig = []

                    for _, row in df.iterrows():
                        rsi = row['rsi']
                        complete = bool(row['complete'])

                        # don’t change state on incomplete/missing RSI rows
                        if not complete or pd.isna(rsi):
                            sig.append(state)
                            continue

                        # Entry when RSI < 60, Exit when RSI >= 60 (matches NOLATCH program)
                        if rsi > 70:
                            state = 1  # Entry signal
                        else:  # rsi < 70
                            state = 0  # Exit signal

                        sig.append(state)

                    df['signal'] = sig
                    processed[indicator_key] = df
            return processed
            
        except Exception as e:
            print(f"Error loading data: {e}")
            self.master.title(f"Enhanced Candle Viewer - {self.current_date} - Error: {str(e)[:50]}...")
            return None

    def create_dataframe(self, candles):
        """Create DataFrame from candle data"""
        df_list = []
        for ts in sorted(candles.keys()):
            candle = candles[ts]
            df_list.append({
                'timestamp': ts,
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'volume': candle['volume'],
                'complete': candle['complete']
            })
        return pd.DataFrame(df_list)

    def get_rsi_from_candles(self, candles):
        """Extract RSI values from candles that already have RSI calculated"""
        rsi_values = []
        for ts in sorted(candles.keys()):
            candle = candles[ts]
            rsi = candle.get('rsi', 100)  # Default to 100 if RSI not available
            rsi_values.append(rsi)
        return rsi_values
    
    def calculate_rsi_signal(self, rsi_value):
        """Calculate signal based on RSI value - matches NOLATCH program logic"""
        if pd.isna(rsi_value) or rsi_value is None:
            return 0
        if rsi_value < 60:
            return 1  # Entry signal
        else:  # rsi_value >= 60
            return 0  # Exit signal

    def update_tabs(self, processed_data):
        """Update tabs with processed data"""
        for indicator_key, df in processed_data.items():
            if indicator_key not in self.strike_pair_tabs:
                self.create_new_tab(indicator_key)
            self.update_treeview(indicator_key, df)

    def create_new_tab(self, indicator_key):
        """Create new tab for strike pair"""
        frame = ttk.Frame(self.notebook)
        # Use shorter name for tab display
        tab_name = indicator_key.replace('CE_', 'C/').replace('PE', 'P')
        self.notebook.add(frame, text=tab_name)
        
        columns = ('timestamp', 'open', 'high', 'low', 'close', 'volume', 'complete', 'rsi', 'signal')
        tree = ttk.Treeview(frame, columns=columns, show='headings')
        
        for col in columns:
            tree.heading(col, text=col.replace('_', ' ').title())
            tree.column(col, width=100, anchor='center')
        
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        self.strike_pair_tabs[indicator_key] = (frame, tree)

    def update_treeview(self, indicator_key, df):
        """Update treeview with data"""
        _, tree = self.strike_pair_tabs[indicator_key]
        tree.delete(*tree.get_children())
        
        for _, row in df.iterrows():
            values = (
                row['timestamp'],
                f"{row['open']:.2f}",
                f"{row['high']:.2f}",
                f"{row['low']:.2f}",
                f"{row['close']:.2f}",
                row['volume'],
                row['complete'],
                f"{row['rsi']:.2f}" if not pd.isna(row['rsi']) else "N/A",
                row['signal']
            )
            
            # Color code based on signal
            tag = "entry" if row['signal'] == 1 else "exit"
            tree.insert('', 'end', values=values, tags=(tag,))
        
        # Configure tag colors
        tree.tag_configure("entry", background="#e6ffe6")  # Light green for entry signal
        tree.tag_configure("exit", background="#ffe6e6")   # Light red for exit signal

    def scan_available_dates(self):
        """Scan for available date folders"""
        try:
            base_dir = "candle_data"
            if os.path.exists(base_dir):
                self.available_dates = []
                for item in os.listdir(base_dir):
                    date_path = os.path.join(base_dir, item)
                    pickle_file = os.path.join(date_path, "strike_pair_candles.pkl")
                    if os.path.isdir(date_path) and os.path.exists(pickle_file):
                        self.available_dates.append(item)
                
                self.available_dates.sort(reverse=True)  # Most recent first
                dates_text = f"Available dates: {', '.join(self.available_dates[:5])}"
                if len(self.available_dates) > 5:
                    dates_text += f" (+{len(self.available_dates)-5} more)"
                self.available_dates_var.set(dates_text)
            else:
                self.available_dates_var.set("No candle_data directory found")
        except Exception as e:
            self.available_dates_var.set(f"Error scanning dates: {e}")
    
    def load_selected_date(self):
        """Load data for the selected date"""
        try:
            new_date = self.date_var.get()
            # Validate date format
            datetime.strptime(new_date, '%Y-%m-%d')
            self.change_date(new_date)
        except ValueError:
            messagebox.showerror("Invalid Date", "Please enter date in YYYY-MM-DD format")
    
    def load_previous_day(self):
        """Load previous day's data"""
        try:
            current = datetime.strptime(self.current_date, '%Y-%m-%d')
            previous = current - timedelta(days=1)
            new_date = previous.strftime('%Y-%m-%d')
            self.date_var.set(new_date)
            self.change_date(new_date)
        except Exception as e:
            messagebox.showerror("Error", f"Could not load previous day: {e}")
    
    def load_next_day(self):
        """Load next day's data"""
        try:
            current = datetime.strptime(self.current_date, '%Y-%m-%d')
            next_day = current + timedelta(days=1)
            new_date = next_day.strftime('%Y-%m-%d')
            self.date_var.set(new_date)
            self.change_date(new_date)
        except Exception as e:
            messagebox.showerror("Error", f"Could not load next day: {e}")
    
    def load_today(self):
        """Load today's data"""
        today = datetime.today().strftime('%Y-%m-%d')
        self.date_var.set(today)
        self.change_date(today)
    
    def change_date(self, new_date):
        """Change to a different date"""
        self.current_date = new_date
        self.data_dir = f"candle_data/{self.current_date}"
        self.input_file = f"{self.data_dir}/strike_pair_candles.pkl"
        self.last_mod_time = None
        
        # Clear existing tabs
        for tab_id in self.notebook.tabs():
            self.notebook.forget(tab_id)
        self.strike_pair_tabs.clear()
        
        # Check if file exists
        if not os.path.exists(self.input_file):
            self.master.title(f"Enhanced Candle Viewer - {self.current_date} - No Data Found")
            messagebox.showwarning("No Data", f"No candle data found for {self.current_date}")
        else:
            self.master.title(f"Enhanced Candle Viewer - {self.current_date} - Loading...")
        
        # Refresh available dates
        self.scan_available_dates()
    
    def update_gui(self):
        """Update GUI with latest data"""
        processed_data = self.load_and_process_data()
        if processed_data:
            self.update_tabs(processed_data)
            
            # Update window title with data count
            count = len(processed_data)
            self.master.title(f"Enhanced Candle Viewer - {self.current_date} - {count} Strike Pairs")
        
        # Only auto-refresh if viewing today's data (real-time mode)
        if self.current_date == datetime.today().strftime('%Y-%m-%d'):
            self.master.after(1000, self.update_gui)
        else:
            # For historical data, refresh less frequently
            self.master.after(5000, self.update_gui)

if __name__ == "__main__":
    root = tk.Tk()
    root.geometry("1200x800")  # Set a good default size
    app = EnhancedCandleGUI(root)
    root.mainloop()
