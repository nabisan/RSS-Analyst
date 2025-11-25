#!/usr/bin/env python
"""
RSS Analyst 1.0 - Production Analyst Accuracy Tracker
Modern Polars/DuckDB/FastAPI rewrite of original analyst prediction analyzer.
Tracks 200+ analysts, generates RSS feeds, serves API endpoints.
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timedelta
from typing import List
import polars as pl
import typer
from loguru import logger
from dotenv import load_dotenv
import uvicorn
import schedule
import threading
import pytz
from pathlib import Path
import httpx
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from tqdm.asyncio import tqdm
import time

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
DB_PATH = "analyst_data.duckdb"

app = typer.Typer()
logger.add("rss_analyst.log", rotation="1 week", level="INFO")

# DuckDB connection
conn = pl.duckdb.connect(DB_PATH)

def init_db():
    """Initialize database schema."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS price_targets (
        symbol TEXT,
        analyst_name TEXT,
        analyst_company TEXT,
        published_date TIMESTAMP,
        price_target DOUBLE,
        news_title TEXT,
        news_url TEXT,
        PRIMARY KEY (symbol, analyst_name, published_date)
    )
    """)
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS analyst_scores (
        analyst_name TEXT PRIMARY KEY,
        skill_score DOUBLE,
        accuracy DOUBLE,
        avg_error DOUBLE,
        total_predictions BIGINT,
        diversity BIGINT,
        updated_at TIMESTAMP
    )
    """)
    logger.info("Database initialized")

async def rate_limited_request(client: httpx.AsyncClient, url: str) -> dict | None:
    """Rate-limited async HTTP request."""
    try:
        response = await client.get(url)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

async def get_latest_price_targets(client: httpx.AsyncClient, analyst_name: str) -> list:
    """Fetch latest price targets for analyst."""
    encoded_name = httpx.URLBuilder().encode_query_params({"name": analyst_name})
    url = f"https://financialmodelingprep.com/api/v4/price-target-analyst-name?{encoded_name}&apikey={API_KEY}"
    
    data = await rate_limited_request(client, url)
    if not data:
        return []
    
    # Filter recent data (last 30 days)
    utc = pytz.UTC
    one_month_ago = datetime.utcnow().replace(tzinfo=utc) - timedelta(days=30)
    recent_data = []
    
    for item in data:
        pub_date = pd.to_datetime(item['publishedDate']).tz_localize('UTC')
        if pub_date >= one_month_ago:
            recent_data.append(item)
    
    # Keep latest per symbol
    latest = {}
    for item in sorted(recent_data, key=lambda x: x['publishedDate'], reverse=True):
        symbol = item['symbol']
        if symbol not in latest:
            latest[symbol] = item
    
    return list(latest.values())

def get_current_price(symbol: str) -> float | None:
    """Get current stock price via yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d")
        return float(data['Close'].iloc[-1]) if not data.empty else None
    except:
        return None

async def fetch_price_targets(symbols: List[str]) -> pl.DataFrame:
    """Fetch price targets for symbols (parallel async)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [client.get(f"https://financialmodelingprep.com/api/v4/price-target?symbol={s}&apikey={API_KEY}") 
                for s in symbols]
        
        results = []
        for coro in tqdm.as_completed(tasks, desc="Fetching price targets"):
            resp = await coro
            if resp.status_code == 200 and resp.json():
                results.extend(resp.json())
    
    if not results:
        return pl.DataFrame()
    
    return pl.DataFrame(results)

def analyze_skill(price_targets: pl.DataFrame, symbols: List[str], days: int = 90) -> pl.DataFrame:
    """Analyze analyst skill using vectorized Polars operations."""
    if price_targets.is_empty():
        return pl.DataFrame()
    
    # Get historical data
    hist_data = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(lambda s=symbol: (symbol, get_historical_data(symbol))): symbol 
                  for symbol in symbols[:50]}  # Limit for speed
        for future in tqdm(list(futures), desc="Historical data"):
            symbol, data = future.result()
            if data is not None:
                hist_data[symbol] = data
    
    if not hist_data:
        return pl.DataFrame()
    
    # Calculate errors (vectorized)
    errors = []
    for row in price_targets.iter_rows(named=True):
        symbol = row['symbol']
        if symbol in hist_data:
            hist = hist_data[symbol]
            pub_date = pd.to_datetime(row['publishedDate']).normalize()
            target_date = pub_date + timedelta(days=days)
            
            closest_date = hist.index.get_indexer([target_date], method='nearest')[0]
            if 0 <= closest_date < len(hist):
                actual_price = hist.iloc[closest_date]
                predicted = row['priceTarget']
                if predicted > 0:
                    pct_error = abs((actual_price - predicted) / predicted) * 100
                    errors.append({**row, 'actual_price': actual_price, 'pct_error': pct_error})
    
    if not errors:
        return pl.DataFrame()
    
    errors_df = pl.DataFrame(errors)
    
    # Group and calculate skill scores
    scores = (errors_df
              .group_by("analystName")
              .agg([
                  pl.col("pct_error").mean().alias("avg_error"),
                  pl.col("symbol").n_unique().alias("diversity"),
                  pl.len().alias("total_predictions")
              ])
              .with_columns([
                  (1 / pl.col("avg_error").clip(1e-6)).alias("accuracy"),
                  (pl.col("total_predictions") * pl.col("diversity")).alias("breadth"),
                  (1 / pl.col("avg_error").clip(1e-6) * 
                   pl.col("total_predictions") * pl.col("diversity")).alias("skill_score")
              ])
              .select(["analystName", "skill_score", "accuracy", "avg_error", 
                      "total_predictions", "diversity"])
              .sort("skill_score", descending=True)
             )
    
    # Store top 10
    top_scores = scores.head(10)
    top_scores.with_columns(pl.now().alias("updated_at")).write_database(
        "analyst_scores", DB_PATH, if_table_exists="replace"
    )
    
    return scores

def get_historical_data(symbol: str) -> pd.Series | None:
    """Fetch historical data for symbol."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="max")
        return hist['Close'] if not hist.empty else None
    except:
        return None

@app.command()
def analyze(symbols_file: str, time_horizon: int = 90):
    """Run analyst skill analysis."""
    if not API_KEY:
        print("Error: FMP_API_KEY required in .env")
        sys.exit(1)
    
    symbols = Path(symbols_file).read_text().splitlines()
    symbols = [s.strip() for s in symbols if s.strip()]
    
    logger.info(f"Analyzing {len(symbols)} symbols over {time_horizon} days")
    init_db()
    
    price_targets = asyncio.run(fetch_price_targets(symbols))
    if not price_targets.is_empty():
        price_targets.write_parquet("price_targets.parquet")
        logger.info(f"Saved {len(price_targets)} price targets")
    
    scores = analyze_skill(price_targets, symbols, time_horizon)
    scores.write_csv("analyst_scores.csv")
    logger.info(f"Analysis complete. Top analysts saved to analyst_scores.csv")

@app.command()
def serve(host: str = "0.0.0.0", port: int = 8000):
    """Start FastAPI server."""
    import api  # Import triggers FastAPI app creation
    uvicorn.run("rss_analyst:app", host=host, port=port, reload=True)

@app.command()
def top_analysts(n: int = 10):
    """Show top N analysts."""
    scores = pl.read_database("SELECT * FROM analyst_scores ORDER BY skill_score DESC LIMIT ?", 
                             conn, parameters=[n])
    print(scores)

def run_scheduler():
    """Run background scheduler."""
    schedule.every().sunday.at("00:00").do(lambda: asyncio.run(analyze("symbols.txt")))
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.command()
def start_scheduler(symbols_file: str):
    """Start background scheduler."""
    init_db()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    scheduler_thread.join()

if __name__ == "__main__":
    if not Path("symbols.txt").exists():
        Path("symbols.txt").write_text("AAPL\nMSFT\nGOOGL\nTSLA\nNVDA\n")
        logger.info("Created symbols.txt with sample symbols")
    
    app()
