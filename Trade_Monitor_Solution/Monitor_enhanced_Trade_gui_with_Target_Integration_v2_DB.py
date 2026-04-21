import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os, re, ast
from datetime import datetime, timedelta
import csv
import sqlite3

try:
    import pyautogui
    SCREENSHOT_AVAILABLE = True
except ImportError:
    SCREENSHOT_AVAILABLE = False

from Monitor_log_parser_service import LogParserService


class CleanTradeMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("Enhanced Trade Monitor - Database Edition")
        self.root.geometry("1500x800")
        
        # Initialize database connection
        self.db_service = LogParserService()
        self.ensure_database_ready()
        
        # Configure styles
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.configure_styles()
        
        # Initialize date
        self.current_date = datetime.today().strftime('%Y-%m-%d')
        self.available_dates = []
        
        # Create GUI components
        self.create_date_navigation()
        self.create_status_panel()
        self.create_trade_grid()
        self.create_controls()
        
        # Initialize data
        self.trades = []
        self.current_prices = {}
        self.current_spot = 0.0
        self.current_strike = 0
        
        # Load available dates
        self.load_available_dates()
        
        # Start updates
        self.update_data()
    
    def ensure_database_ready(self):
        """Check if database exists, if not parse the log file"""
        if not os.path.exists(self.db_service.db_file):
            response = messagebox.showinfo(
                "First Time Setup",
                "Database not found. This will parse your log file (one-time operation).\n\n"
                "This may take 1-2 minutes for large logs.\n\n"
                "Click OK to proceed..."
            )
            self.parse_logs_with_progress()
        else:
            self.db_service.init_database()
    
    def parse_logs_with_progress(self):
        """Parse logs with progress indication"""
        # Create progress window
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Parsing Trading Logs...")
        progress_window.geometry("400x150")
        progress_window.transient(self.root)
        progress_window.grab_set()
        
        ttk.Label(progress_window, text="Parsing trading log file...", font=('Helvetica', 11, 'bold')).pack(pady=10)
        
        progress_var = tk.DoubleVar()
        progress_bar = ttk.Progressbar(progress_window, variable=progress_var, maximum=100)
        progress_bar.pack(padx=20, pady=10, fill=tk.X)
        
        status_label = ttk.Label(progress_window, text="Initializing...", font=('Helvetica', 9))
        status_label.pack(pady=5)
        
        def update_progress(lines, percent):
            progress_var.set(percent)
            status_label.config(text=f"Processed: {lines} lines ({percent}%)")
            progress_window.update_idletasks()
        
        try:
            trades_count = self.db_service.parse_full_log(progress_callback=update_progress)
            
            progress_var.set(100)
            status_label.config(text=f"✓ Complete! {trades_count} trades parsed")
            progress_window.update_idletasks()
            
            self.root.after(1000, progress_window.destroy)
            messagebox.showinfo("Success", f"Database created successfully with {trades_count} trades!")
        
        except Exception as e:
            messagebox.showerror("Error", f"Failed to parse logs: {e}")
            progress_window.destroy()
    
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
        ttk.Button(control_frame, text="🔄 Rebuild DB", command=self.rebuild_database).pack(side=tk.LEFT, padx=2)
        
        if SCREENSHOT_AVAILABLE:
            ttk.Button(control_frame, text="📷 Screenshot", command=self.take_screenshot).pack(side=tk.LEFT, padx=2)
        
        # Auto-refresh toggle for today's date
        self.auto_refresh = tk.BooleanVar(value=True)
        ttk.Checkbutton(control_frame, text="Auto-refresh", variable=self.auto_refresh).pack(side=tk.RIGHT, padx=10)
    
    def load_available_dates(self):
        """Load available dates from database"""
        try:
            self.available_dates = self.db_service.get_available_dates()
            
            if self.available_dates:
                recent_dates = self.available_dates[:3]
                status_text = f"Available: {', '.join(recent_dates)}"
                if len(self.available_dates) > 3:
                    status_text += f" (+{len(self.available_dates)-3} more)"
            else:
                status_text = "No trading data found"
            
            self.dates_status_var.set(status_text)
        except Exception as e:
            self.dates_status_var.set(f"Error loading dates: {str(e)[:30]}...")
    
    def load_trades(self):
        """Load trades from database for the current date"""
        self.trades = []
        self.current_prices = {}
        
        try:
            # Get trades for date from database
            trades_rows = self.db_service.get_trades_for_date(self.current_date)
            self.trades = [dict(row) for row in trades_rows]
            
            # Get latest prices
            prices_data = self.db_service.get_latest_prices(self.current_date)
            self.current_prices = {k: v['price'] for k, v in prices_data.items()}
            
            # Get current state
            state = self.db_service.get_current_state()
            self.current_spot = state['spot']
            self.current_strike = state['strike']
        
        except Exception as e:
            print(f"Error loading trades: {e}")
    
    def calculate_mtm(self, trade):
        if trade.get('exit_price'):
            exit_price = trade['exit_price']
            entry_price = trade['entry_price']
            if exit_price is not None and entry_price is not None:
                return (float(entry_price) - float(exit_price)) * trade.get('qty', 20)
        
        current_price = self.current_prices.get(trade['trade_id'])
        entry_price = trade.get('entry_price')
        if current_price is not None and entry_price is not None:
            return (float(entry_price) - float(current_price)) * trade.get('qty', 20)
        
        return 0.0

    def update_data(self):
        self.load_trades()
        
        # Update status panel - safe float conversion
        spot = self.current_spot if self.current_spot else 0.0
        strike = self.current_strike if self.current_strike else 0
        
        self.spot_label.config(text=f"{spot:.2f}")
        self.strike_label.config(text=str(strike))
        
        # Update treeview
        self.refresh_tree_display()
        
        # Schedule next update only for today and if auto-refresh is enabled
        is_today = self.current_date == datetime.today().strftime('%Y-%m-%d')
        if is_today and self.auto_refresh.get():
            self.root.after(2000, self.update_data)
    
    def refresh_tree_display(self):
        """Refresh the tree display with current trade data"""
        self.tree.delete(*self.tree.get_children())
        total_mtm = 0
        
        for trade in self.trades:
            mtm = self.calculate_mtm(trade)
            total_mtm += mtm
            
            values = (
                trade.get('trade_id', ''),
                trade.get('entry_time', ''),
                str(trade.get('strike', '')),
                f"{float(trade.get('entry_spot', 0)):.2f}" if trade.get('entry_spot') is not None else "N/A",
                "Short" if trade.get('signal_entry') == 1 else ("Long" if trade.get('signal_entry') else ""),
                f"{float(trade.get('entry_price', 0)):.2f}" if trade.get('entry_price') is not None else "N/A",
                trade.get('exit_time', "") or "",
                f"{float(trade.get('exit_spot', 0)):.2f}" if trade.get('exit_spot') is not None else "",
                "Target" if trade.get('signal_exit') == 'target' else ("Exit" if trade.get('signal_exit') == 0 else ""),
                f"{float(trade.get('exit_price', 0)):.2f}" if trade.get('exit_price') is not None else "",
                f"{float(self.current_prices.get(trade.get('trade_id'), 0.0)):.2f}",
                trade.get('qty', 20),
                f"{mtm:+.2f}"
            )
            
            tag = "green" if mtm >= 0 else "red"
            self.tree.insert("", tk.END, values=values, tags=(tag,))
        
        self.tree.tag_configure("green", foreground="green")
        self.tree.tag_configure("red", foreground="red")
        self.total_mtm.config(text=f"{total_mtm:.2f}")
    
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
        
        # Check if data exists for this date
        if new_date not in self.available_dates:
            messagebox.showwarning("No Data", f"No trading data found for {new_date}")
        
        # Update window title
        self.root.title(f"Enhanced Trade Monitor - {new_date}")
        
        # Refresh data
        self.update_data()
    
    def rebuild_database(self):
        """Rebuild database from scratch"""
        response = messagebox.askyesno(
            "Rebuild Database",
            "This will re-parse the trading log file.\n\n"
            "Current database will be replaced.\n\n"
            "Continue?"
        )
        
        if response:
            try:
                if os.path.exists(self.db_service.db_file):
                    os.remove(self.db_service.db_file)
                
                self.parse_logs_with_progress()
                self.load_available_dates()
                self.update_data()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to rebuild database: {e}")
    
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
                initialname=f"trades_{self.current_date}.csv"
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
                            f"{trade['entry_spot']:.2f}" if trade['entry_spot'] else "N/A",
                            "Short" if trade['signal_entry'] == 1 else "Long",
                            f"{trade['entry_price']:.2f}" if trade['entry_price'] else "N/A",
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
        import traceback
        traceback.print_exc()
