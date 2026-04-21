import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os, re, ast
from datetime import datetime, timedelta
import csv
import sys

try:
    import pyautogui
    SCREENSHOT_AVAILABLE = True
except ImportError:
    SCREENSHOT_AVAILABLE = False


class LogIndex:
    """Builds and maintains an index of log file offsets by date for fast random access."""
    def __init__(self, log_path="trading_log.log", index_path="log_index.json"):
        self.log_path = log_path
        self.index_path = index_path
        self.date_offsets = {}          # date -> (start_offset, end_offset)
        self.file_size = 0
        self.last_mtime = 0

    def _parse_date_from_line(self, line):
        """Extract date (YYYY-MM-DD) from the beginning of a log line."""
        try:
            # Format: "YYYY-MM-DD HH:MM:SS,mmm - LEVEL - EVENT - {...}"
            parts = line.split(' - ', 1)
            if parts:
                timestamp = parts[0].strip()
                date_part = timestamp.split()[0] if timestamp else None
                if date_part and re.match(r'\d{4}-\d{2}-\d{2}', date_part):
                    return date_part
        except:
            pass
        return None

    def build_index(self):
        """Scan the entire log file and record start/end offsets for each date."""
        if not os.path.exists(self.log_path):
            self.date_offsets = {}
            return

        self.date_offsets = {}
        current_date = None
        start_offset = None
        end_offset = None

        with open(self.log_path, 'r', encoding='utf-8') as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break

                line_date = self._parse_date_from_line(line)
                if not line_date:
                    continue

                if line_date != current_date:
                    # Close previous date range
                    if current_date is not None:
                        self.date_offsets[current_date] = (start_offset, offset - 1)

                    # Start new date
                    current_date = line_date
                    start_offset = offset

            # Handle last date
            if current_date is not None and start_offset is not None:
                self.date_offsets[current_date] = (start_offset, offset)

        self.file_size = os.path.getsize(self.log_path)
        self.last_mtime = os.path.getmtime(self.log_path)

    def save_index(self):
        """Save the index to a JSON file."""
        data = {
            'file_size': self.file_size,
            'last_mtime': self.last_mtime,
            'offsets': {date: list(offs) for date, offs in self.date_offsets.items()}
        }
        with open(self.index_path, 'w') as f:
            json.dump(data, f)

    def load_index(self):
        """Load index from file if it matches the current log file."""
        if not os.path.exists(self.index_path):
            return False
        try:
            with open(self.index_path, 'r') as f:
                data = json.load(f)
            if (data['file_size'] == os.path.getsize(self.log_path) and
                data['last_mtime'] == os.path.getmtime(self.log_path)):
                self.date_offsets = {date: tuple(offs) for date, offs in data['offsets'].items()}
                self.file_size = data['file_size']
                self.last_mtime = data['last_mtime']
                return True
        except:
            pass
        return False

    def update_index(self):
        """Incrementally update index for new data appended to the log."""
        if not os.path.exists(self.log_path):
            return
        current_size = os.path.getsize(self.log_path)
        if current_size <= self.file_size:
            return

        # Seek to the end of previously indexed data
        with open(self.log_path, 'r', encoding='utf-8') as f:
            f.seek(self.file_size)
            current_date = None
            start_offset = None
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                line_date = self._parse_date_from_line(line)
                if not line_date:
                    continue
                if line_date != current_date:
                    if current_date is not None and start_offset is not None:
                        # If we already have this date, extend its end offset
                        if current_date in self.date_offsets:
                            old_start, _ = self.date_offsets[current_date]
                            self.date_offsets[current_date] = (old_start, offset - 1)
                        else:
                            self.date_offsets[current_date] = (start_offset, offset - 1)
                    current_date = line_date
                    start_offset = offset
            # Handle last line
            if current_date is not None and start_offset is not None:
                if current_date in self.date_offsets:
                    old_start, _ = self.date_offsets[current_date]
                    self.date_offsets[current_date] = (old_start, offset)
                else:
                    self.date_offsets[current_date] = (start_offset, offset)

        self.file_size = current_size
        self.last_mtime = os.path.getmtime(self.log_path)

    def get_range(self, date):
        """Return (start_offset, end_offset) for the given date, or None."""
        return self.date_offsets.get(date)

    def ensure_index(self):
        """Load or build the index, then update if file grew."""
        if not self.load_index():
            self.build_index()
        else:
            self.update_index()
        self.save_index()


class CleanTradeMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("Enhanced Trade Monitor")
        self.root.geometry("1500x800")
        
        # Configure styles
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.configure_styles()
        
        # Initialize date
        self.current_date = datetime.today().strftime('%Y-%m-%d')
        self.available_dates = []
        
        # Log index for fast date-based access
        self.log_index = LogIndex()
        self.log_index.ensure_index()
        
        # Data storage
        self.trades = []                # list of trade dicts from log
        self.user_qty = {}              # trade_id -> user-modified quantity
        self.default_qty = 20           # default quantity
        self.current_prices = {}
        self.current_spot = 0.0
        self.current_strike = 0
        
        # Create GUI components
        self.create_date_navigation()
        self.create_status_panel()
        self.create_trade_grid()
        self.create_controls()
        
        # Scan for available dates (from index)
        self.update_available_dates()
        
        # Start updates
        self.update_data()
        
    def configure_styles(self):
        self.style.configure("TLabel", font=('Helvetica', 10))
        self.style.configure("Header.TLabel", font=('Helvetica', 10, 'bold'))
        self.style.configure("Green.TLabel", foreground="green")
        self.style.configure("Red.TLabel", foreground="red")
        self.style.configure("Status.TFrame", background="#f0f0f0")
    
    def create_date_navigation(self):
        """Create date navigation controls"""
        nav_frame = ttk.Frame(self.root)
        nav_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(nav_frame, text="Date:", font=('Helvetica', 10, 'bold')).pack(side=tk.LEFT, padx=5)
        
        self.date_var = tk.StringVar(value=self.current_date)
        self.date_entry = ttk.Entry(nav_frame, textvariable=self.date_var, width=12, font=('Helvetica', 10))
        self.date_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(nav_frame, text="Load Date", command=self.load_selected_date).pack(side=tk.LEFT, padx=2)
        ttk.Button(nav_frame, text="◀ Prev", command=self.load_previous_day).pack(side=tk.LEFT, padx=2)
        ttk.Button(nav_frame, text="Next ▶", command=self.load_next_day).pack(side=tk.LEFT, padx=2)
        ttk.Button(nav_frame, text="Today", command=self.load_today).pack(side=tk.LEFT, padx=2)
        
        # Available dates display
        self.dates_status_var = tk.StringVar(value="Loading available dates...")
        ttk.Label(nav_frame, textvariable=self.dates_status_var, font=('Helvetica', 9)).pack(side=tk.LEFT, padx=20)
        
    def create_status_panel(self):
        status_frame = ttk.Frame(self.root, style="Status.TFrame")
        status_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(status_frame, text="Current Spot:", style="Header.TLabel").grid(row=0, column=0, padx=5)
        self.spot_label = ttk.Label(status_frame, text="0.00", style="TLabel")
        self.spot_label.grid(row=0, column=1, padx=5)
        
        ttk.Label(status_frame, text="Current Strike:", style="Header.TLabel").grid(row=0, column=2, padx=5)
        self.strike_label = ttk.Label(status_frame, text="0", style="TLabel")
        self.strike_label.grid(row=0, column=3, padx=5)
        
        ttk.Label(status_frame, text="Updated Time:", style="Header.TLabel").grid(row=0, column=4, padx=5)
        self.Update_time = ttk.Label(status_frame, text="0", style="TLabel")
        self.Update_time.grid(row=0, column=5, padx=5)
        
        ttk.Label(status_frame, text="Total MTM:", style="Header.TLabel").grid(row=0, column=6, padx=5)
        self.total_mtm = ttk.Label(status_frame, text="0", style="TLabel")
        self.total_mtm.grid(row=0, column=7, padx=5)
        
        ttk.Label(status_frame, text="Viewing Date:", style="Header.TLabel").grid(row=0, column=8, padx=5)
        self.viewing_date_label = ttk.Label(status_frame, text=self.current_date, style="TLabel")
        self.viewing_date_label.grid(row=0, column=9, padx=5)
        
    def create_trade_grid(self):
        # Create container frame for tree and scrollbar
        tree_frame = ttk.Frame(self.root)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Trade columns
        self.columns = (
            "Trade ID", "Entry Time", "Strike", "Entry Spot", "Signal Entry", "Entry Price",
            "Exit Time", "Exit Spot", "Signal Exit", "Exit Price", 
            "Current Price", "QTY", "MTM"
        )
        
        self.tree = ttk.Treeview(tree_frame, columns=self.columns, show="headings", selectmode="browse")
        
        # Configure columns
        col_widths = [120, 140, 70, 90, 90, 90, 140, 90, 90, 90, 90, 60, 90]
        for col, width in zip(self.columns, col_widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor=tk.CENTER)
        
        # Make QTY column editable
        self.tree.bind('<Double-1>', self.on_tree_double_click)
        
        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        # Pack tree and scrollbars
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
    def create_controls(self):
        control_frame = ttk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(control_frame, text="🔄 Refresh", command=self.update_data).pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="📊 Export CSV", command=self.export_csv).pack(side=tk.LEFT, padx=2)
        
        if SCREENSHOT_AVAILABLE:
            ttk.Button(control_frame, text="📷 Screenshot", command=self.take_screenshot).pack(side=tk.LEFT, padx=2)
        
        # Default quantity setting
        ttk.Label(control_frame, text="Default Qty:").pack(side=tk.LEFT, padx=(20,2))
        self.default_qty_var = tk.IntVar(value=self.default_qty)
        default_qty_spin = ttk.Spinbox(control_frame, from_=1, to=10000, textvariable=self.default_qty_var, width=6)
        default_qty_spin.pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="Apply to All", command=self.apply_default_qty_to_all).pack(side=tk.LEFT, padx=2)
        
        # Auto-refresh toggle for today's date
        self.auto_refresh = tk.BooleanVar(value=True)
        ttk.Checkbutton(control_frame, text="Auto-refresh", variable=self.auto_refresh).pack(side=tk.RIGHT, padx=10)

    def update_available_dates(self):
        """Update the list of available dates from the index."""
        self.available_dates = sorted(self.log_index.date_offsets.keys(), reverse=True)
        if self.available_dates:
            recent = self.available_dates[:3]
            status = f"Available: {', '.join(recent)}"
            if len(self.available_dates) > 3:
                status += f" (+{len(self.available_dates)-3} more)"
        else:
            status = "No trading data found"
        self.dates_status_var.set(status)

    def load_trades_for_date(self, date):
        """
        Read only the lines belonging to the given date using the log index.
        Returns a list of trade dictionaries (entries and exits combined).
        Also extracts latest spot/strike from PRICE_UPDATE lines.
        """
        trades_dict = {}
        current_spot = 0.0
        current_strike = 0

        date_range = self.log_index.get_range(date)
        if not date_range:
            return trades_dict, current_spot, current_strike

        start_offset, end_offset = date_range

        with open(self.log_index.log_path, 'r', encoding='utf-8') as f:
            f.seek(start_offset)
            # Read up to end_offset (inclusive)
            while f.tell() <= end_offset:
                line = f.readline()
                if not line:
                    break

                try:
                    parts = line.split(" - ", 4)
                    if len(parts) <= 3:
                        continue
                    
                    timestamp, _, event_type, log_data = parts
                    event_type = event_type.strip()
                    
                    # Clean and parse JSON data
                    log_data = log_data.strip()
                    log_data = log_data.replace("'", '"')
                    log_data = re.sub(r'Timestamp\((["\'])(.*?)\1\)', r'\2', log_data)
                    
                    try:
                        entry = json.loads(log_data)
                    except json.JSONDecodeError:
                        entry = ast.literal_eval(log_data)

                    if event_type == "ENTRY":
                        trade_id = entry.get('trade_id')
                        if not trade_id:
                            continue
                            
                        trades_dict[trade_id] = {
                            'trade_id': trade_id,
                            'entry_time': timestamp.split(',')[0],
                            'strike': entry['strike'],
                            'entry_spot': entry['spot'],
                            'signal_entry': entry['signal']['signal'],
                            'entry_price': entry['call_ltp'] + entry['put_ltp'],
                            'qty': self.default_qty,   # will be overridden by user_qty later
                            'exit_time': "",
                            'exit_spot': "",
                            'signal_exit': "",
                            'exit_price': ""
                        }

                    elif event_type == "EXIT" or event_type == "TARGET_EXIT":
                        trade_id = entry.get('trade_id')
                        if trade_id in trades_dict:
                            signal_value = entry['signal']['signal'] if event_type == "EXIT" else "TARGET"
                            exit_signal = 'target' if signal_value == "TARGET" else signal_value
                            trades_dict[trade_id].update({
                                'exit_time': timestamp.split(',')[0],
                                'exit_spot': entry['exit_spot'],
                                'signal_exit': exit_signal,
                                'exit_price': entry['call_ltp'] + entry['put_ltp']
                            })

                    elif event_type == "PRICE_UPDATE":
                        trade_id = entry.get('trade_id')
                        if trade_id is not None:
                            self.current_prices[trade_id] = entry['strangle_price']
                        current_spot = entry.get('spot', current_spot)
                        current_strike = entry.get('strikes', current_strike)

                except Exception as e:
                    print(f"Error processing line: {line.strip()} - {str(e)}")
                    continue

        return trades_dict, current_spot, current_strike

    def merge_trades_with_user_qty(self, new_trades_dict):
        """
        Merge newly loaded trades with existing user quantity overrides.
        Returns a list of trade dicts with qty set appropriately.
        """
        merged = []
        for trade_id, trade in new_trades_dict.items():
            trade['qty'] = self.user_qty.get(trade_id, self.default_qty)
            merged.append(trade)
        return merged

    def load_trades(self):
        """Load trades for the current date using the index."""
        self.trades = []
        self.current_prices = {}
        
        # Update index in case log file grew (especially for today)
        self.log_index.update_index()
        
        # Load only data for the current date
        trades_dict, spot, strike = self.load_trades_for_date(self.current_date)
        
        self.trades = self.merge_trades_with_user_qty(trades_dict)
        self.current_spot = spot
        self.current_strike = strike

    def calculate_mtm(self, trade):
        if trade.get('exit_price'):
            return (float(trade['exit_price']) - float(trade['entry_price'])) * trade['qty']
        else:
            current_price = self.current_prices.get(trade['trade_id'], trade['entry_price'])
            return (current_price - float(trade['entry_price'])) * trade['qty']

    def update_data(self):
        self.load_trades()
        
        # Update status panel
        self.spot_label.config(text=f"{self.current_spot:.2f}")
        self.strike_label.config(text=str(self.current_strike))
        self.Update_time.config(text=str(datetime.now().strftime('%H:%M:%S')))
        self.viewing_date_label.config(text=self.current_date)
        
        # Update treeview
        self.refresh_tree_display()
        
        # Schedule next update only for today and if auto-refresh is enabled
        is_today = self.current_date == datetime.today().strftime('%Y-%m-%d')
        if is_today and self.auto_refresh.get():
            self.root.after(2000, self.update_data)  # 2 second interval
    
    def refresh_tree_display(self):
        """Refresh the tree display with current trade data"""
        self.tree.delete(*self.tree.get_children())
        total_mtm = 0
        
        for trade in self.trades:
            mtm = self.calculate_mtm(trade)
            total_mtm += mtm
            
            values = (
                trade['trade_id'],
                trade['entry_time'],
                trade['strike'],
                f"{trade['entry_spot']:.2f}",
                "Long" if trade['signal_entry'] == 1 else "Short",
                f"{trade['entry_price']:.2f}",
                trade.get('exit_time', ""),
                f"{trade.get('exit_spot', ''):.2f}" if trade.get('exit_spot') else "",
                "Target" if trade.get('signal_exit') == 'target' else ("Exit" if trade.get('signal_exit') == 0 else ""),
                f"{trade.get('exit_price', ''):.2f}" if trade.get('exit_price') else "",
                f"{self.current_prices.get(trade['trade_id'], 0.0):.2f}",
                trade['qty'],
                f"{mtm:+.2f}"
            )
            
            tag = "green" if mtm >= 0 else "red"
            self.tree.insert("", tk.END, values=values, tags=(tag,), iid=str(trade['trade_id']))
            
        self.tree.tag_configure("green", foreground="green")
        self.tree.tag_configure("red", foreground="red")
        self.total_mtm.config(text=f"{total_mtm:.2f}")

    # ---------- Editable QTY ----------
    def on_tree_double_click(self, event):
        """Handle double-click on tree cell to edit QTY."""
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        column = self.tree.identify_column(event.x)
        # Column 12 is QTY (1-based indexing in identify_column)
        if column != "#12":
            return
        item = self.tree.identify_row(event.y)
        if not item:
            return

        # Get current value
        values = self.tree.item(item, 'values')
        if not values:
            return
        trade_id = values[0]  # Trade ID is first column
        current_qty = values[11]  # QTY column

        # Create an entry widget for editing
        x, y, width, height = self.tree.bbox(item, column="#12")
        if not x:
            return

        self.edit_entry = ttk.Entry(self.tree, width=10)
        self.edit_entry.place(x=x, y=y, width=width, height=height)
        self.edit_entry.insert(0, str(current_qty))
        self.edit_entry.focus_set()
        self.edit_entry.bind('<Return>', lambda e: self.save_qty_edit(item, trade_id))
        self.edit_entry.bind('<FocusOut>', lambda e: self.save_qty_edit(item, trade_id))
        self.edit_entry.bind('<Escape>', lambda e: self.edit_entry.destroy())

    def save_qty_edit(self, item, trade_id):
        """Save the edited quantity and update the display."""
        if hasattr(self, 'edit_entry') and self.edit_entry.winfo_exists():
            try:
                new_qty = int(self.edit_entry.get())
                if new_qty <= 0:
                    raise ValueError
                # Update user_qty dict
                self.user_qty[trade_id] = new_qty
                # Update the trade in self.trades
                for trade in self.trades:
                    if trade['trade_id'] == trade_id:
                        trade['qty'] = new_qty
                        break
                # Refresh only the affected row
                self.refresh_tree_display()
            except ValueError:
                messagebox.showerror("Invalid Quantity", "Please enter a positive integer.")
            finally:
                self.edit_entry.destroy()
                delattr(self, 'edit_entry')

    def apply_default_qty_to_all(self):
        """Set all trades to the current default quantity."""
        new_default = self.default_qty_var.get()
        if new_default <= 0:
            messagebox.showerror("Invalid Quantity", "Default quantity must be positive.")
            return
        self.default_qty = new_default
        # Update all trades and clear user overrides
        for trade in self.trades:
            trade['qty'] = new_default
        self.user_qty.clear()
        self.refresh_tree_display()

    # ---------- Date navigation ----------
    def scan_available_dates(self):
        """Already handled by index; kept for compatibility."""
        self.update_available_dates()
    
    def load_selected_date(self):
        """Load data for selected date"""
        try:
            new_date = self.date_var.get()
            datetime.strptime(new_date, '%Y-%m-%d')  # Validate format
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
        self.trades = []
        self.current_prices = {}
        
        # Check if data exists for this date (optional warning)
        if new_date not in self.log_index.date_offsets:
            messagebox.showwarning("No Data", f"No trading data found for {new_date}")
        
        # Update window title
        self.root.title(f"Enhanced Trade Monitor - {new_date}")
        
        # Refresh data
        self.update_data()
    
    def take_screenshot(self):
        """Take screenshot of the entire window"""
        try:
            # Update the window to ensure everything is rendered
            self.root.update_idletasks()
            
            # Get window geometry
            x = self.root.winfo_rootx()
            y = self.root.winfo_rooty()
            width = self.root.winfo_width()
            height = self.root.winfo_height()
            
            # Take screenshot of the window area
            screenshot = pyautogui.screenshot(region=(x, y, width, height))
            
            # Create screenshots directory if it doesn't exist
            os.makedirs("screenshots", exist_ok=True)
            
            # Save with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"screenshots/trade_monitor_{self.current_date}_{timestamp}.png"
            screenshot.save(filename)
            
            messagebox.showinfo("Screenshot Saved", f"Screenshot saved as: {filename}")
                
        except Exception as e:
            messagebox.showerror("Screenshot Error", f"Failed to take screenshot: {str(e)}")
    
    def export_csv(self):
        """Export trade data to CSV"""
        try:
            if not self.trades:
                messagebox.showwarning("No Data", "No trade data to export")
                return
                
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Export Trade Data",
                initialfile=f"trades_{self.current_date}.csv"
            )
            
            if filename:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write headers
                    writer.writerow(self.columns)
                    
                    # Write trade data
                    for trade in self.trades:
                        mtm = self.calculate_mtm(trade)
                        
                        row = [
                            trade['trade_id'],
                            trade['entry_time'],
                            trade['strike'],
                            f"{trade['entry_spot']:.2f}",
                            "Long" if trade['signal_entry'] == 1 else "Short",
                            f"{trade['entry_price']:.2f}",
                            trade.get('exit_time', ''),
                            f"{trade.get('exit_spot', ''):.2f}" if trade.get('exit_spot') else '',
                            "Target" if trade.get('signal_exit') == 'target' else ("Exit" if trade.get('signal_exit') == 0 else ""),
                            f"{trade.get('exit_price', ''):.2f}" if trade.get('exit_price') else '',
                            f"{self.current_prices.get(trade['trade_id'], 0.0):.2f}",
                            trade['qty'],
                            f"{mtm:+.2f}"
                        ]
                        
                        writer.writerow(row)
                
                messagebox.showinfo("Export Complete", f"Trade data exported to: {filename}")
                
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export CSV: {str(e)}")


if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = CleanTradeMonitor(root)
        root.mainloop()
    except Exception as e:
        print(f"Error starting application: {e}")
        messagebox.showerror("Startup Error", f"Failed to start application: {str(e)}")
