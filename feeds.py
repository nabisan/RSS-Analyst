#!/usr/bin/env python
"""
RSS Analyst 1.0 - RSS Feed Generator
Generates RSS feeds from top analysts with sentiment analysis and quant filters.
"""

from feedgen.feed import FeedGenerator
from datetime import datetime
import polars as pl
from loguru import logger
from pathlib import Path
from typing import List, Dict, Any
import pytz
from dateutil import parser as dateparser
import httpx
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
DB_PATH = "analyst_data.duckdb"

conn = pl.duckdb.connect(DB_PATH)

def get_top_analysts(min_predictions: int = 5, limit: int = 10) -> pl.DataFrame:
    """Get top analysts from database."""
    query = """
    SELECT * FROM analyst_scores 
    WHERE total_predictions >= ?
    ORDER BY skill_score DESC 
    LIMIT ?
    """
    try:
        return pl.read_database(query, conn, parameters=[min_predictions, limit])
    except:
        logger.warning("No analyst scores found")
        return pl.DataFrame()

async def get_latest_reports(analyst_name: str) -> List[Dict[str, Any]]:
    """Fetch latest reports for analyst."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        encoded_name = httpx.QueryParams({"name": analyst_name})
        url = f"https://financialmodelingprep.com/api/v4/price-target-analyst-name{encoded_name}&apikey={API_KEY}"
        
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                return []
            
            data = resp.json()
            if not data:
                return []
            
            # Filter recent (30 days) and dedupe by symbol
            utc = pytz.UTC
            cutoff = datetime.utcnow().replace(tzinfo=utc) - timedelta(days=30)
            
            recent = []
            latest_per_symbol = {}
            
            for item in data:
                pub_date = dateparser.parse(item['publishedDate'])
                if pub_date.tzinfo is None:
                    pub_date = utc.localize(pub_date)
                
                if pub_date >= cutoff:
                    symbol = item['symbol']
                    if symbol not in latest_per_symbol:
                        latest_per_symbol[symbol] = item
                        recent.append(item)
            
            return recent
        except Exception as e:
            logger.error(f"Error fetching reports for {analyst_name}: {e}")
            return []

def analyze_sentiment(text: str) -> float:
    """Simple keyword-based quant sentiment (VADER alternative)."""
    quant_positive = ['bullish', 'buy', 'target raised', 'outperform', 'overweight', 'upgrade']
    quant_negative = ['bearish', 'sell', 'target lowered', 'underperform', 'underweight', 'downgrade']
    
    text_lower = text.lower()
    pos_score = sum(1 for word in quant_positive if word in text_lower)
    neg_score = sum(1 for word in quant_negative if word in text_lower)
    
    if pos_score + neg_score == 0:
        return 0.0
    return (pos_score - neg_score) / (pos_score + neg_score)

def generate_rss_feed() -> str:
    """Generate RSS feed from top analysts."""
    logger.info("Generating RSS feed...")
    
    top_analysts = get_top_analysts()
    if top_analysts.is_empty():
        logger.warning("No top analysts available")
        return ""
    
    # Fetch latest reports concurrently
    loop = asyncio.get_event_loop()
    tasks = [get_latest_reports(row['analyst_name']) for row in top_analysts.iter_rows(named=True)]
    all_reports = loop.run_until_complete(asyncio.gather(*tasks))
    
    feed_entries = []
    for i, reports in enumerate(all_reports):
        if not reports:
            continue
        
        analyst_row = top_analysts.row(i, named=True)
        for report in reports[:3]:  # Top 3 per analyst
            sentiment = analyze_sentiment(report.get('newsTitle', '') + ' ' + report.get('description', ''))
            
            entry = {
                'title': f"{analyst_row['analyst_name']} ({analyst_row['accuracy']:.1%} acc) - {report.get('newsTitle', 'Price Target')}",
                'link': report.get('newsURL', ''),
                'pub_date': dateparser.parse(report.get('publishedDate', '')),
                'description': f"""
                <div>
                    <h3>{report.get('newsTitle', '')}</h3>
                    <p><strong>Symbol:</strong> {report.get('symbol', '')} | <strong>Target:</strong> ${report.get('priceTarget', 0):.2f}</p>
                    <p><strong>Accuracy:</strong> {analyst_row['accuracy']:.1%} | <strong>Sentiment:</strong> {sentiment:.2f}</p>
                    <p><strong>Predictions:</strong> {analyst_row['total_predictions']} | <strong>Diversity:</strong> {analyst_row['diversity']}</p>
                </div>
                """,
                'analyst_info': analyst_row
            }
            feed_entries.append(entry)
    
    if not feed_entries:
        logger.warning("No feed entries generated")
        return ""
    
    # Generate RSS
    fg = FeedGenerator()
    fg.title('RSS Analyst 2.0 - Top Quant Picks')
    fg.link(href='http://localhost:8000/rss', rel='self')
    fg.description('Daily picks from top-performing financial analysts')
    fg.language('en')
    
    for entry in sorted(feed_entries, key=lambda x: x['pub_date'], reverse=True)[:50]:
        fe = fg.add_entry()
        fe.title(entry['title'])
        fe.link(href=entry['link'])
        fe.pubDate(entry['pub_date'])
        fe.description(entry['description'])
    
    rss_content = fg.rss_str(pretty=True).decode('utf-8')
    Path('top_analysts_feed.xml').write_text(rss_content, encoding='utf-8')
    
    logger.info(f"RSS feed generated: {len(feed_entries)} entries")
    return rss_content

def generate_quant_rss(keywords: List[str] = None) -> str:
    """Generate quant-focused RSS with keyword filtering."""
    if keywords is None:
        keywords = ['microstructure', 'HFT', 'VWAP', 'orderflow', 'quant', 'algo']
    
    logger.info(f"Generating quant RSS for keywords: {keywords}")
    
    top_analysts = get_top_analysts(limit=20)
    quant_entries = []
    
    for row in top_analysts.iter_rows(named=True):
        # Simulate quant filtering (in production, query with keywords)
        quant_entries.append({
            'title': f"Quant Alert: {row['analyst_name']} ({row['skill_score']:.0f})",
            'description': f"High-skill analyst signal. Accuracy: {row['accuracy']:.1%}",
            'link': f"http://localhost:8000/analyst/{row['analyst_name']}"
        })
    
    fg = FeedGenerator()
    fg.title('RSS Analyst - Quant Signals')
    fg.description('Quant-focused analyst signals')
    
    for entry in quant_entries[:25]:
        fe = fg.add_entry()
        fe.title(entry['title'])
        fe.link(href=entry['link'])
        fe.pubDate(datetime.utcnow())
        fe.description(entry['description'])
    
    content = fg.rss_str(pretty=True).decode('utf-8')
    Path('quant_signals.xml').write_text(content)
    logger.info("Quant RSS generated")
    
    return content

def main():
    """Generate all RSS feeds."""
    generate_rss_feed()
    generate_quant_rss()
    logger.success("All RSS feeds generated")

if __name__ == "__main__":
    main()
