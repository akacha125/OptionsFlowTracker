import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import time
from datetime import datetime
import threading

# If you want scheduling:
try:
    import schedule
except ImportError:
    pass

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Unusual Options Volume (Multi-Page)",
    layout="wide"
)

DB_NAME = "options_data.db"
SYMBOLS = ["AAPL", "TSLA", "MSFT", "AMZN", "SPY", "QQQ", "NVDA", "META", "GOOGL"]

if "scheduler_running" not in st.session_state:
    st.session_state.scheduler_running = False
if "refresh_count" not in st.session_state:
    st.session_state.refresh_count = 0

# --------------------------------------------------
# HELPER: MULTISELECT WITH "ALL"
# --------------------------------------------------
def multiselect_with_all(label, options, sidebar=True):
    """
    A helper function that shows a multiselect box with an "All" option.
    If "All" is chosen, returns the full list. Otherwise, returns the chosen subset.
    """
    extended_opts = ["All"] + sorted(options)
    default_val = ["All"]
    if sidebar:
        selected = st.sidebar.multiselect(label, extended_opts, default=default_val)
    else:
        selected = st.multiselect(label, extended_opts, default=default_val)

    if "All" in selected:
        return list(options)
    else:
        return [x for x in selected if x != "All"]

# --------------------------------------------------
# 1. DATABASE SETUP
# --------------------------------------------------
def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS options_snapshots (
                snapshot_time TEXT,
                symbol TEXT,
                type TEXT,
                expiry TEXT,
                strike REAL,
                volume REAL,
                bid REAL,
                ask REAL,
                current_price REAL
            )
        """)
        conn.commit()

def store_snapshot(df: pd.DataFrame, snapshot_time: str):
    if df.empty:
        return
    with sqlite3.connect(DB_NAME) as conn:
        df_to_store = df.copy()
        df_to_store["snapshot_time"] = snapshot_time
        df_to_store = df_to_store[
            [
                "snapshot_time", "Symbol", "Type", "Expiry",
                "Strike", "Volume", "Bid", "Ask", "current_price"
            ]
        ]
        df_to_store.to_sql("options_snapshots", conn, if_exists="append", index=False)

def get_latest_snapshot_time():
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(snapshot_time) FROM options_snapshots")
        row = c.fetchone()
        return row[0] if row and row[0] else None

def get_all_snapshots():
    query = """
        SELECT snapshot_time,
               symbol AS Symbol,
               type AS Type,
               expiry AS Expiry,
               strike AS Strike,
               volume AS Volume,
               bid AS Bid,
               ask AS Ask,
               current_price AS current_price
          FROM options_snapshots
    """
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql_query(query, conn)
    return df

def get_snapshot_data(snapshot_time: str):
    query = """
        SELECT symbol AS Symbol,
               type AS Type,
               expiry AS Expiry,
               strike AS Strike,
               volume AS Volume,
               bid AS Bid,
               ask AS Ask,
               current_price AS current_price
          FROM options_snapshots
         WHERE snapshot_time = ?
    """
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql_query(query, conn, params=[snapshot_time])
    return df

# --------------------------------------------------
# 2. FETCHING / COMPARISON
# --------------------------------------------------
@st.cache_data(ttl=300)
def fetch_options_data(symbols):
    all_data = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            current_price = ticker.info.get("regularMarketPrice", None)
            if current_price is None:
                hist = ticker.history(period="1d")
                if not hist.empty:
                    current_price = hist["Close"].iloc[-1]

            if not ticker.options:
                continue

            for expiry in ticker.options:
                opt_chain = ticker.option_chain(expiry)
                for opt_type, chain_df in [("Call", opt_chain.calls), ("Put", opt_chain.puts)]:
                    for _, row in chain_df.iterrows():
                        all_data.append({
                            "Symbol": symbol,
                            "Type": opt_type,
                            "Expiry": expiry,
                            "Strike": row.get("strike", 0),
                            "Volume": row.get("volume", 0),
                            "Bid": row.get("bid", 0),
                            "Ask": row.get("ask", 0),
                            "current_price": current_price,
                        })
            time.sleep(0.2)
        except Exception as e:
            st.warning(f"Error fetching data for {symbol}: {e}")

    df = pd.DataFrame(all_data)
    if not df.empty:
        df.sort_values("Volume", ascending=False, inplace=True)
    return df

def find_unusual_volume(new_df, old_df, ratio_thr, diff_thr):
    if new_df.empty or old_df.empty:
        return pd.DataFrame([])

    keys = ["Symbol", "Type", "Expiry", "Strike"]
    merged = pd.merge(new_df, old_df, on=keys, how="outer", suffixes=("", "_old"))

    merged["Volume"] = merged["Volume"].fillna(0)
    merged["Volume_old"] = merged["Volume_old"].fillna(0)

    merged["Volume_Diff"] = merged["Volume"] - merged["Volume_old"]
    def ratio_func(row):
        if row["Volume_old"] > 0:
            return row["Volume"] / row["Volume_old"]
        else:
            return float("inf")
    merged["Volume_Ratio"] = merged.apply(ratio_func, axis=1)

    condition = (
        (merged["Volume_Ratio"] >= ratio_thr) &
        (merged["Volume_Diff"] >= diff_thr)
    )
    unusual = merged[condition].copy()

    if unusual.empty:
        return unusual

    keep_cols = [
        "Symbol", "Type", "Expiry", "Strike",
        "Volume_old", "Volume", "Volume_Diff", "Volume_Ratio",
        "Bid", "Ask", "current_price"
    ]
    existing_cols = [c for c in keep_cols if c in unusual.columns]
    unusual = unusual[condition][existing_cols].sort_values("Volume_Diff", ascending=False)
    return unusual

# --------------------------------------------------
# 3. ALERTS
# --------------------------------------------------
def send_alerts(unusual_df):
    if unusual_df.empty:
        print("[ALERT] No unusual volume found.")
        return
    print("[ALERT] Unusual volume detected!")
    limited = unusual_df.head(5)
    print(limited.to_string(index=False))

# --------------------------------------------------
# 4. BACKGROUND SCHEDULER
# --------------------------------------------------
def background_fetch_job():
    while True:
        interval_minutes = 1
        try:
            fetch_options_data.clear()
            new_snapshot_df = fetch_options_data(SYMBOLS)

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not new_snapshot_df.empty:
                old_time = get_latest_snapshot_time()
                old_df = pd.DataFrame([])
                if old_time:
                    old_df = get_snapshot_data(old_time)

                store_snapshot(new_snapshot_df, now_str)

                ratio_thr = 2.0
                diff_thr = 1000
                unusual_df = find_unusual_volume(new_snapshot_df, old_df, ratio_thr, diff_thr)
                if not unusual_df.empty:
                    send_alerts(unusual_df)

                print(f"[Scheduler] Snapshot stored at {now_str}, {len(new_snapshot_df)} rows.")
            else:
                print("[Scheduler] Empty new snapshot. No data to store.")

        except Exception as ex:
            print(f"[Scheduler] Error: {ex}")

        for _ in range(interval_minutes * 6):
            time.sleep(10)

def start_scheduler():
    if st.session_state.scheduler_running:
        st.write("Scheduler is already running.")
        return
    st.session_state.scheduler_running = True
    thread = threading.Thread(target=background_fetch_job, daemon=True)
    thread.start()
    st.write("Background scheduler thread started.")

# --------------------------------------------------
# PAGE 1: Options Flow Tracker
# --------------------------------------------------
def page_options_flow():
    st.title("Options Flow Tracker")

    # Create two tabs within this page: "Flow Data" and "Settings"
    tab1, tab2 = st.tabs(["Flow Data", "Settings"])

    with tab1:
        st.subheader("Filtered Options Data")

        # --- Load all snapshots ---
        df_all = get_all_snapshots()
        if df_all.empty:
            st.info("No snapshots in DB yet. Go to 'Settings' tab to fetch data.")
            return

        df_all.sort_values("snapshot_time", inplace=True)

        st.sidebar.header("Filters")
        # Symbol
        unique_symbols = df_all["Symbol"].unique()
        selected_symbols = multiselect_with_all("Symbols", unique_symbols)
        # Type
        unique_types = df_all["Type"].unique()
        selected_types = multiselect_with_all("Option Types", unique_types)
        # Expiry
        unique_expiries = df_all["Expiry"].unique()
        selected_expiries = multiselect_with_all("Expiries", unique_expiries)
        # Strike
        unique_strikes = df_all["Strike"].unique().astype(str)
        selected_strikes = multiselect_with_all("Strikes", unique_strikes)

        filtered = df_all[
            df_all["Symbol"].isin(selected_symbols) &
            df_all["Type"].isin(selected_types) &
            df_all["Expiry"].isin(selected_expiries)
        ].copy()
        filtered["Strike_str"] = filtered["Strike"].astype(str)
        filtered = filtered[filtered["Strike_str"].isin(selected_strikes)]

        if filtered.empty:
            st.warning("No data matching the selected filters.")
        else:
            # Remove duplicates
            filtered.drop_duplicates(
                subset=["Symbol","Type","Expiry","Strike","Volume","Bid","Ask","current_price"],
                inplace=True
            )
            # Sort by Volume desc
            filtered = filtered.sort_values("Volume", ascending=False)

            # Reorder columns
            filtered.rename(columns={"Expiry":"Expiry Date"}, inplace=True)
            final_cols = [
                "Symbol",
                "current_price",
                "Strike",
                "Bid",
                "Ask",
                "Type",
                "Expiry Date",
                "Volume"
            ]
            existing_cols = [c for c in final_cols if c in filtered.columns]
            display_df = filtered[existing_cols].copy()
            display_df.rename(columns={"current_price":"Current Price"}, inplace=True)
            display_df.drop(columns=["Strike_str"], errors="ignore", inplace=True)

            st.dataframe(display_df.reset_index(drop=True))

    with tab2:
        # This is the old "Settings" page content
        st.subheader("Scheduler Controls")
        if st.button("Start Background Scheduler"):
            start_scheduler()

        st.subheader("Manual Snapshot Fetch")
        if st.button("Fetch Snapshot Now"):
            fetch_options_data.clear()
            new_snapshot = fetch_options_data(SYMBOLS)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if new_snapshot.empty:
                st.warning("No data returned from Yahoo.")
            else:
                old_time = get_latest_snapshot_time()
                old_df = pd.DataFrame([])
                if old_time:
                    old_df = get_snapshot_data(old_time)

                store_snapshot(new_snapshot, now_str)
                st.success(f"Snapshot stored at {now_str}, {len(new_snapshot)} rows.")

                ratio_thr = 2.0
                diff_thr = 1000
                unusual_df = find_unusual_volume(new_snapshot, old_df, ratio_thr, diff_thr)
                if unusual_df.empty:
                    st.info("No unusual volume found.")
                else:
                    st.success(f"Detected {len(unusual_df)} unusual volume rows.")
                    st.dataframe(unusual_df)

                send_alerts(unusual_df)

        st.write("---")
        st.write(f"**Refresh Count (session):** {st.session_state.refresh_count}")
        if st.session_state.scheduler_running:
            st.info("Background scheduler is running in a separate thread.")

# --------------------------------------------------
# PAGE 2: Stock Chart
# --------------------------------------------------
@st.cache_data
def load_stock_data(symbol, period, interval, after_hours, refresh_counter):
    """
    The combination of these arguments forms the cache key.
    Changing any => re-fetch from yfinance.
    """
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval, prepost=after_hours)
    return df

@st.cache_data(ttl=300)
def fetch_stock_history(symbol, period, interval, after_hours):
    ticker = yf.Ticker(symbol)
    return ticker.history(period=period, interval=interval, prepost=after_hours)

def page_stock_chart():
    if "stock_refresh" not in st.session_state:
        st.session_state["stock_refresh"] = 0

    SYMBOLS = ["AAPL","MSFT","TSLA","SPY","QQQ"]
    PERIODS = ["1d","5d","1mo","6mo","1y","5y","max"]
    INTERVALS = ["1m","5m","15m","30m","1h","1d","1wk","1mo"]

    st.sidebar.header("Filters (Stock Chart)")
    symbol = st.sidebar.selectbox("Symbol", SYMBOLS, index=1)  # e.g. "MSFT"
    period = st.sidebar.selectbox("Period", PERIODS, index=3)  # e.g. "1d"
    interval = st.sidebar.selectbox("Interval", INTERVALS, index=2)  # e.g. "15m"
    after_hours = st.sidebar.checkbox("Include After-Hours Data?", value=False)
    chart_type = st.sidebar.radio("Chart Type", ["Line","Candlestick"], index=1)

    # Refresh
    if st.sidebar.button("Refresh Now"):
        st.session_state["stock_refresh"] += 1

    auto_refresh = st.sidebar.checkbox("Auto-refresh every 15 seconds", value=False)

    # Clear cache button
    if st.sidebar.button("Clear Cache"):
        load_stock_data.clear()

    # Fetch data
    if symbol:
        with st.spinner("Loading stock chart..."):
            df = load_stock_data(symbol, period, interval, after_hours, st.session_state["stock_refresh"])

        if df.empty:
            st.warning(f"No price data found for {symbol}.")
        else:
            st.header("Stock Chart & Price (Line or Candlestick)")
            st.write(f"**Symbol**: {symbol}, **Last Price**: {df['Close'].iloc[-1]:.2f}")
            # Build chart
            fig = go.Figure()
            if chart_type == "Line":
                fig.add_trace(go.Scatter(x=df.index, y=df["Close"], mode="lines", name="Close"))
            else:
                fig.add_trace(
                    go.Candlestick(
                        x=df.index,
                        open=df["Open"],
                        high=df["High"],
                        low=df["Low"],
                        close=df["Close"],
                        name=symbol
                    )
                )

            fig.update_layout(
                title=f"{symbol} - {period} ({interval})",
                xaxis_title="Date",
                yaxis_title="Price",
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(df.tail(10))

    if auto_refresh:
        time.sleep(15)
        st.session_state["stock_refresh"] += 1
# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    init_db()

    # Two pages in the sidebar:
    # 1) "Options Flow Tracker"
    # 2) "Stock Chart"
    pages = ["Options Flow Tracker", "Stock Chart"]
    chosen_page = st.sidebar.selectbox("Navigation", pages, index=0)

    if chosen_page == "Options Flow Tracker":
        page_options_flow()
    else:
        page_stock_chart()

    # If you want any common footer info:
    st.write("---")
    st.write(f"**Refresh Count (session):** {st.session_state.refresh_count}")
    if st.session_state.scheduler_running:
        st.info("Background scheduler is running in a separate thread.")

if __name__ == "__main__":
    main()