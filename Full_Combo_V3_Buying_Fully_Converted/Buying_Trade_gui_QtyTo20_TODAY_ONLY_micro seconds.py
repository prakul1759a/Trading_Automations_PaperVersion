import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os, re, ast
from pathlib import Path
from datetime import datetime, timedelta
import csv

try:
    import pyautogui
    SCREENSHOT_AVAILABLE = True
except ImportError:
    SCREENSHOT_AVAILABLE = False

class CleanTradeMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("BSE - RSI 50-60 Tgt-400")
        self.root.geometry("1500x800")
        
        # Configure styles
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.configure_styles()

        # Initialize paths and date - TODAY ONLY
        self.base_dir = Path(__file__).resolve().parent
        self.log_path = self.base_dir / "trading_log.log"
        self.today = datetime.today().strftime('%Y-%m-%d')

        # Create GUI components
        self.create_status_panel()
        self.create_trade_grid()
        self.create_controls()

        # Initialize data
        self.trades = []
        self.trades_dict = {}
        self.current_prices = {}
        self.current_spot = 0.0
        self.current_strike = 0
        self.last_file_position = 0  # For incremental reading

        # Start updates
        self.update_data()
        
    def configure_styles(self):
        self.style.configure("TLabel", font=('Helvetica', 10))
        self.style.configure("Header.TLabel", font=('Helvetica', 10, 'bold'))
        self.style.configure("Green.TLabel", foreground="green")
        self.style.configure("Red.TLabel", foreground="red")
        self.style.configure("Status.TFrame", background="#f0f0f0")
    
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

        ttk.Label(status_frame, text="Today:", style="Header.TLabel").grid(row=0, column=8, padx=5)
        self.today_label = ttk.Label(status_frame, text=self.today, style="TLabel")
        self.today_label.grid(row=0, column=9, padx=5)
        
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
        
        if SCREENSHOT_AVAILABLE:
            ttk.Button(control_frame, text="📷 Screenshot", command=self.take_screenshot).pack(side=tk.LEFT, padx=2)
        
        # Auto-refresh toggle for today's date
        self.auto_refresh = tk.BooleanVar(value=True)
        ttk.Checkbutton(control_frame, text="Auto-refresh", variable=self.auto_refresh).pack(side=tk.RIGHT, padx=10)

    def format_time_with_2decimals(self, timestamp):
        """Format timestamp to show time with 2 digits after seconds
        Input: '2025-11-25 09:15:30,123456'
        Output: '2025-11-25 09:15:30:12'
        """
        try:
            if ',' in timestamp:
                base_time, milliseconds = timestamp.split(',', 1)
                two_digits = milliseconds[:2]
                return f"{base_time}:{two_digits}"
            else:
                return timestamp
        except:
            return timestamp.split(',')[0] if ',' in timestamp else timestamp

    def load_trades(self):
        """Load only TODAY's trades - incremental reading for maximum performance"""
        if not self.log_path.exists():
            return

        current_spot = self.current_spot
        current_strike = self.current_strike

        # Reset incremental read pointer if log rotated/truncated
        try:
            if self.last_file_position > self.log_path.stat().st_size:
                self.last_file_position = 0
        except OSError:
            self.last_file_position = 0

        with self.log_path.open("r", encoding="utf-8") as f:
            # Seek to last position (incremental read)
            f.seek(self.last_file_position)

            for line in f:
                # Quick date filter: Check first 50 chars for today's date (FAST skip for old data)
                if self.today not in line[:50]:
                    continue
                try:
                    parts = line.split(" - ", 4)
                    if len(parts) <= 3:
                        continue
                    
                    timestamp, _, event_type, log_data = parts
                    timestamp_date = timestamp.split()[0]

                    # Only process today's data
                    if self.today == timestamp_date:
                        event_type = event_type.strip()
                        
                        # Clean and parse JSON data
                        log_data = log_data.strip()
                        log_data = log_data.replace("'", '"')
                        log_data = re.sub(r'Timestamp\((["\'])(.*?)\1\)', r'\2', log_data)
                        
                        try:
                            entry = json.loads(log_data)
                        except json.JSONDecodeError:
                            entry = ast.literal_eval(log_data)

                        # Handle different event types
                        if event_type == "ENTRY":
                            trade_id = entry.get('trade_id')
                            if not trade_id:
                                continue

                            # Apply markup: Entry is SELL, so subtract 10 per leg (total -20)
                            markup = 10
                            # entry_price_with_markup = (entry['call_ltp'] - markup) + (entry['put_ltp'] - markup)
                            entry_price_with_markup = entry['call_ltp'] + entry['put_ltp']  # Disabled markup

                            # Format timestamp to show 2 decimal digits
                            entry_time_formatted = self.format_time_with_2decimals(timestamp)

                            self.trades_dict[trade_id] = {
                                'trade_id': trade_id,
                                'entry_time': entry_time_formatted,
                                'strike': entry['strike'],
                                'entry_spot': entry['spot'],
                                'signal_entry': entry['signal']['signal'],
                                'entry_price': entry_price_with_markup,
                                'qty': 20,
                                'exit_time': "",
                                'exit_spot': "",
                                'signal_exit': "",
                                'exit_price': ""
                            }

                        elif event_type == "EXIT":
                            trade_id = entry.get('trade_id')
                            if trade_id in self.trades_dict:
                                signal_value = entry['signal']['signal']
                                # Handle both regular exits (signal=0) and target exits (signal="TARGET")
                                exit_signal = 'target' if signal_value == "TARGET" else signal_value

                                # Apply markup: Exit is BUY, so add 10 per leg (total +20)
                                markup = 10
                                # exit_price_with_markup = (entry['call_ltp'] + markup) + (entry['put_ltp'] + markup)
                                exit_price_with_markup = entry['call_ltp'] + entry['put_ltp']  # Disabled markup

                                # Format timestamp to show 2 digits after seconds
                                exit_time_formatted = self.format_time_with_2decimals(timestamp)

                                self.trades_dict[trade_id].update({
                                    'exit_time': exit_time_formatted,
                                    'exit_spot': entry['exit_spot'],
                                    'signal_exit': exit_signal,
                                    'exit_price': exit_price_with_markup
                                })

                        elif event_type == "TARGET_EXIT":
                            trade_id = entry.get('trade_id')
                            if trade_id in self.trades_dict:
                                # Apply markup: Exit is BUY, so add 10 per leg (total +20)
                                markup = 10
                                # exit_price_with_markup = (entry['call_ltp'] + markup) + (entry['put_ltp'] + markup)
                                exit_price_with_markup = entry['call_ltp'] + entry['put_ltp']  # Disabled markup

                                # Format timestamp to show 2 digits after seconds
                                exit_time_formatted = self.format_time_with_2decimals(timestamp)

                                self.trades_dict[trade_id].update({
                                    'exit_time': exit_time_formatted,
                                    'exit_spot': entry['exit_spot'],
                                    'signal_exit': 'target',
                                    'exit_price': exit_price_with_markup
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

            # Save file position for next incremental read
            self.last_file_position = f.tell()

        # Update class state
        self.trades = list(self.trades_dict.values())
        self.current_spot = current_spot
        self.current_strike = current_strike
             
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
        self.Update_time.config(text=str(datetime.now()))

        # Update treeview
        self.refresh_tree_display()

        # Schedule next update if auto-refresh is enabled
        if self.auto_refresh.get():
            self.root.after(1000, self.update_data)
    
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
            self.tree.insert("", tk.END, values=values, tags=(tag,))
            
        self.tree.tag_configure("green", foreground="green")
        self.tree.tag_configure("red", foreground="red")
        self.total_mtm.config(text=f"{total_mtm:.2f}")

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
            filename = f"screenshots/trade_monitor_{self.today}_{timestamp}.png"
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
                initialfile=f"trades_{self.today}.csv"
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
