import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os
import csv
from datetime import datetime

try:
    import pyautogui
    SCREENSHOT_AVAILABLE = True
except ImportError:
    SCREENSHOT_AVAILABLE = False

# ── Helpers ────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _safe_float(v):
    try:
        return float(v) if v not in (None, '', 'nan') else 0.0
    except Exception:
        return 0.0


def load_smg_state(date_str):
    state_dir = os.path.join(BASE_DIR, 'trading_states_slicedmango')
    path      = os.path.join(state_dir, f"{date_str}_smg_state.json")
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def count_active_slots(state):
    count = 0
    for basket in state.get('baskets', {}).values():
        for slot in basket.get('slots', {}).values():
            if slot.get('is_active'):
                count += 1
    return count


# ── Dashboard (today-only, incremental CSV read) ───────────────────────────────

class CleanTradeMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("SMG - SlicedMango Monitor (Today)")
        self.root.geometry("1600x800")

        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.configure_styles()

        self.today              = datetime.today().strftime('%Y-%m-%d')
        self.trades_dict        = {}          # slot_id -> slot_dict
        self.basket_counter     = 0
        self.active_slots_count = 0
        self.last_file_position = 0           # incremental CSV read pointer
        self.csv_headers        = None        # parsed once from header row

        self.create_status_panel()
        self.create_trade_grid()
        self.create_controls()

        self.update_data()

    def configure_styles(self):
        self.style.configure("TLabel",        font=('Helvetica', 10))
        self.style.configure("Header.TLabel", font=('Helvetica', 10, 'bold'))
        self.style.configure("Status.TFrame", background="#f0f0f0")

    # ── Status panel ──────────────────────────────────────────────────────────

    def create_status_panel(self):
        sf = ttk.Frame(self.root, style="Status.TFrame")
        sf.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(sf, text="Basket Counter:", style="Header.TLabel").grid(row=0, column=0, padx=5)
        self.basket_label = ttk.Label(sf, text="0", style="TLabel")
        self.basket_label.grid(row=0, column=1, padx=5)

        ttk.Label(sf, text="Active Slots:", style="Header.TLabel").grid(row=0, column=2, padx=5)
        self.active_label = ttk.Label(sf, text="0", style="TLabel")
        self.active_label.grid(row=0, column=3, padx=5)

        ttk.Label(sf, text="Updated Time:", style="Header.TLabel").grid(row=0, column=4, padx=5)
        self.Update_time = ttk.Label(sf, text="—", style="TLabel")
        self.Update_time.grid(row=0, column=5, padx=5)

        ttk.Label(sf, text="Total MTM:", style="Header.TLabel").grid(row=0, column=6, padx=5)
        self.total_mtm = ttk.Label(sf, text="0", style="TLabel")
        self.total_mtm.grid(row=0, column=7, padx=5)

        ttk.Label(sf, text="Today:", style="Header.TLabel").grid(row=0, column=8, padx=5)
        self.today_label = ttk.Label(sf, text=self.today, style="TLabel")
        self.today_label.grid(row=0, column=9, padx=5)

    # ── Trade grid ────────────────────────────────────────────────────────────

    def create_trade_grid(self):
        tf = ttk.Frame(self.root)
        tf.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.columns = (
            "Slot ID", "Entry Time", "Strike", "VWAP Ref", "Level",
            "Entry Price", "Exit Time", "Exit Reason", "Field %", "Exit Price",
            "Current Price", "QTY", "MTM"
        )
        col_widths = [110, 140, 70, 90, 90, 90, 140, 120, 60, 90, 90, 50, 90]

        self.tree = ttk.Treeview(tf, columns=self.columns, show="headings", selectmode="browse")
        for col, w in zip(self.columns, col_widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor=tk.CENTER)

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=self.tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

    # ── Controls ──────────────────────────────────────────────────────────────

    def create_controls(self):
        cf = ttk.Frame(self.root)
        cf.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(cf, text="🔄 Refresh",    command=self.update_data).pack(side=tk.LEFT, padx=2)
        ttk.Button(cf, text="📊 Export CSV", command=self.export_csv).pack(side=tk.LEFT, padx=2)
        if SCREENSHOT_AVAILABLE:
            ttk.Button(cf, text="📷 Screenshot", command=self.take_screenshot).pack(side=tk.LEFT, padx=2)

        self.auto_refresh = tk.BooleanVar(value=True)
        ttk.Checkbutton(cf, text="Auto-refresh", variable=self.auto_refresh).pack(side=tk.RIGHT, padx=10)

    # ── Data loading ──────────────────────────────────────────────────────────

    def load_trades(self):
        """Incrementally read new rows from today's smg_slots CSV."""
        compact  = self.today.replace('-', '')
        csv_path = os.path.join(BASE_DIR, f"smg_slots_{compact}.csv")

        if not os.path.exists(csv_path):
            return

        # Reset if file was truncated/rotated
        try:
            file_size = os.path.getsize(csv_path)
            if self.last_file_position > file_size:
                self.last_file_position = 0
                self.trades_dict        = {}
                self.csv_headers        = None
        except OSError:
            self.last_file_position = 0

        with open(csv_path, 'r', encoding='utf-8') as f:
            # First read: parse header row
            if self.last_file_position == 0 or self.csv_headers is None:
                header_line  = f.readline()
                self.csv_headers = [h.strip() for h in header_line.strip().split(',')]
                self.last_file_position = f.tell()
            else:
                f.seek(self.last_file_position)

            reader = csv.DictReader(f, fieldnames=self.csv_headers)
            for row in reader:
                event_type = row.get('event_type', '').strip()
                basket_id  = row.get('basket_id',  '').strip()
                slot_num   = row.get('slot_num',   '').strip()
                if not basket_id or not slot_num:
                    continue
                try:
                    slot_id = f"SMG_{int(basket_id):04d}_{int(slot_num)}"
                except Exception:
                    continue

                pct_raw = _safe_float(row.get('price_field_pct', 0))
                pct_str = f"{pct_raw * 100:.0f}%" if pct_raw else ''

                if event_type == 'entry':
                    entry_time_fmt = self._format_time(row.get('datetime', ''))
                    self.trades_dict[slot_id] = {
                        'trade_id':    slot_id,
                        'entry_time':  entry_time_fmt,
                        'strike':      row.get('ce_strike', ''),
                        'entry_spot':  _safe_float(row.get('vwap_ref', 0)),
                        'signal_entry':row.get('level_name', ''),
                        'entry_price': _safe_float(row.get('entry_price', 0)),
                        'target_price':_safe_float(row.get('target_price', 0)),
                        'sl_price':    _safe_float(row.get('sl_price', 0)),
                        'field_pct':   pct_str,
                        'qty':         20,
                        'exit_time':   '',
                        'exit_spot':   '',
                        'signal_exit': '',
                        'exit_price':  None,
                        'pnl':         0.0,
                    }
                elif event_type in ('force_exit', 'exit') and slot_id in self.trades_dict:
                    pnl_raw = row.get('pnl', '')
                    exit_time_fmt = self._format_time(row.get('datetime', ''))
                    self.trades_dict[slot_id].update({
                        'exit_time':  exit_time_fmt,
                        'exit_spot':  row.get('reason', ''),
                        'signal_exit':row.get('reason', ''),
                        'exit_price': _safe_float(row.get('combined_ltp', 0)),
                        'pnl':        _safe_float(pnl_raw) if pnl_raw else 0.0,
                    })

            self.last_file_position = f.tell()

        # Read SMG state for status bar
        state = load_smg_state(self.today)
        self.basket_counter     = state.get('basket_counter', 0)
        self.active_slots_count = count_active_slots(state)

    @staticmethod
    def _format_time(timestamp):
        """Return time portion with fractional second: '09:49:01:12'"""
        try:
            if ' ' in timestamp:
                date_part, time_part = timestamp.split(' ', 1)
                return time_part[:8]   # HH:MM:SS
            return timestamp
        except Exception:
            return timestamp

    def calculate_mtm(self, trade):
        if trade.get('exit_price'):
            return float(trade.get('pnl', 0))
        return 0.0

    def update_data(self):
        self.load_trades()

        self.basket_label.config(text=str(self.basket_counter))
        self.active_label.config(text=str(self.active_slots_count))
        self.Update_time.config(text=datetime.now().strftime('%H:%M:%S'))

        self.refresh_tree_display()

        if self.auto_refresh.get():
            self.root.after(1000, self.update_data)

    def refresh_tree_display(self):
        self.tree.delete(*self.tree.get_children())
        total_mtm = 0.0

        for trade in self.trades_dict.values():
            mtm = self.calculate_mtm(trade)
            total_mtm += mtm

            current_price = (
                float(trade['exit_price']) if trade.get('exit_price')
                else float(trade.get('entry_price', 0))
            )

            values = (
                trade['trade_id'],
                trade['entry_time'],
                str(trade.get('strike', '')),
                f"{float(trade.get('entry_spot', 0)):.2f}",
                trade.get('signal_entry', ''),
                f"{float(trade.get('entry_price', 0)):.2f}",
                trade.get('exit_time', ''),
                trade.get('exit_spot', ''),
                trade.get('field_pct', ''),
                f"{float(trade.get('exit_price', 0)):.2f}" if trade.get('exit_price') else "",
                f"{current_price:.2f}",
                trade.get('qty', 20),
                f"{mtm:+.2f}",
            )
            tag = "green" if mtm >= 0 else "red"
            self.tree.insert("", tk.END, values=values, tags=(tag,))

        self.tree.tag_configure("green", foreground="green")
        self.tree.tag_configure("red",   foreground="red")
        self.total_mtm.config(text=f"{total_mtm:.2f}")

    # ── Screenshot ────────────────────────────────────────────────────────────

    def take_screenshot(self):
        try:
            self.root.update_idletasks()
            x, y = self.root.winfo_rootx(), self.root.winfo_rooty()
            w, h = self.root.winfo_width(), self.root.winfo_height()
            screenshot = pyautogui.screenshot(region=(x, y, w, h))
            os.makedirs("screenshots", exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fn = f"screenshots/smg_monitor_{self.today}_{ts}.png"
            screenshot.save(fn)
            messagebox.showinfo("Screenshot Saved", f"Saved: {fn}")
        except Exception as e:
            messagebox.showerror("Screenshot Error", str(e))

    # ── Export CSV ────────────────────────────────────────────────────────────

    def export_csv(self):
        try:
            if not self.trades_dict:
                messagebox.showwarning("No Data", "No trade data to export")
                return
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=f"smg_export_{self.today}.csv",
            )
            if filename:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.columns)
                    for trade in self.trades_dict.values():
                        mtm = self.calculate_mtm(trade)
                        current_price = (
                            float(trade['exit_price']) if trade.get('exit_price')
                            else float(trade.get('entry_price', 0))
                        )
                        writer.writerow([
                            trade['trade_id'],
                            trade['entry_time'],
                            trade.get('strike', ''),
                            f"{float(trade.get('entry_spot', 0)):.2f}",
                            trade.get('signal_entry', ''),
                            f"{float(trade.get('entry_price', 0)):.2f}",
                            trade.get('exit_time', ''),
                            trade.get('exit_spot', ''),
                            trade.get('field_pct', ''),
                            f"{float(trade.get('exit_price', 0)):.2f}" if trade.get('exit_price') else "",
                            f"{current_price:.2f}",
                            trade.get('qty', 20),
                            f"{mtm:+.2f}",
                        ])
                messagebox.showinfo("Export Complete", f"Exported to: {filename}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))


if __name__ == "__main__":
    try:
        root = tk.Tk()
        app  = CleanTradeMonitor(root)
        root.mainloop()
    except Exception as e:
        print(f"Error starting application: {e}")
        messagebox.showerror("Startup Error", str(e))