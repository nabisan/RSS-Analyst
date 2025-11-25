#!/usr/bin/env python
"""
RSS Analyst 2.0 - Database Models and Queries
Polars/DuckDB schema management and optimized queries.
"""

import polars as pl
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
import pytz
from dataclasses import dataclass
from typing import TypedDict

DB_PATH = "analyst_data.duckdb"

@dataclass
class AnalystScore:
    """Analyst skill metrics."""
    analyst_name: str
    skill_score: float
    accuracy: float
    avg_error: float
    total_predictions: int
    diversity: int
    updated_at: datetime

@dataclass
class PriceTarget:
    """Individual price target prediction."""
    symbol: str
    analyst_name: str
    analyst_company: str
    published_date: datetime
    price_target: float
    news_title: str
    news_url: str

class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = Path(db_path)
        self.conn = pl.duckdb.connect(db_path)
        self.init_schema()
    
    def init_schema(self):
        """Create optimized tables with indexes."""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS price_targets (
            symbol TEXT,
            analyst_name TEXT,
            analyst_company TEXT,
            published_date TIMESTAMP,
            price_target DOUBLE,
            news_title TEXT,
            news_url TEXT,
            actual_price DOUBLE,
            pct_error DOUBLE,
            sentiment_score DOUBLE,
            PRIMARY KEY (symbol, analyst_name, published_date)
        )
        """)
        
        self.conn.execute("""
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
        
        # Indexes for performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_price_targets_symbol ON price_targets(symbol)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_price_targets_date ON price_targets(published_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_price_targets_analyst ON price_targets(analyst_name)")
        
        logger.info(f"Database initialized: {self.db_path}")

    def insert_price_targets(self, targets: List[Dict[str, Any]]):
        """Bulk insert price targets."""
        if not targets:
            return
        
        df = pl.DataFrame(targets)
        df.write_database(
            "price_targets", 
            self.db_path, 
            if_table_exists="append"
        )
        logger.info(f"Inserted {len(targets)} price targets")

    def update_analyst_scores(self, scores: List[Dict[str, Any]]):
        """Upsert analyst scores."""
        df = pl.DataFrame(scores)
        df.write_database(
            "analyst_scores",
            self.db_path,
            if_table_exists="replace"
        )
        logger.info(f"Updated {len(scores)} analyst scores")

    def get_top_analysts(self, limit: int = 10, min_predictions: int = 5) -> pl.DataFrame:
        """Get top analysts leaderboard."""
        query = """
        SELECT 
            analyst_name,
            skill_score,
            accuracy,
            avg_error,
            total_predictions,
            diversity,
            updated_at
        FROM analyst_scores 
        WHERE total_predictions >= ?
        ORDER BY skill_score DESC 
        LIMIT ?
        """
        return pl.read_database(query, self.conn, parameters=[min_predictions, limit])

    def get_analyst_targets(self, analyst_name: str, days: int = 30) -> pl.DataFrame:
        """Get recent targets for analyst."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = """
        SELECT 
            symbol,
            analyst_name,
            analyst_company,
            published_date,
            price_target,
            news_title,
            news_url,
            actual_price,
            pct_error
        FROM price_targets 
        WHERE analyst_name = ? AND published_date >= ?
        ORDER BY published_date DESC
        """
        return pl.read_database(
            query, self.conn, 
            parameters=[analyst_name, cutoff]
        )

    def search_analysts(self, query: str, limit: int = 20) -> pl.DataFrame:
        """Full-text search analysts."""
        search_query = f"""
        SELECT * FROM analyst_scores 
        WHERE analyst_name ILIKE '%{query}%' OR analyst_company ILIKE '%{query}%'
        ORDER BY skill_score DESC 
        LIMIT ?
        """
        return pl.read_database(search_query, self.conn, parameters=[limit])

    def get_recent_targets(self, limit: int = 100) -> pl.DataFrame:
        """Get most recent price targets."""
        query = """
        SELECT * FROM price_targets 
        ORDER BY published_date DESC 
        LIMIT ?
        """
        return pl.read_database(query, self.conn, parameters=[limit])

    def calculate_leaderboard(self, symbols: List[str], days: int = 90) -> pl.DataFrame:
        """Full leaderboard recalculation."""
        query = """
        SELECT 
            pt.analyst_name,
            AVG(pt.pct_error) as avg_error,
            COUNT(DISTINCT pt.symbol) as diversity,
            COUNT(*) as total_predictions
        FROM price_targets pt
        WHERE pt.published_date >= datetime('now', '-{} days')
        AND pt.symbol = ANY(?)
        GROUP BY pt.analyst_name
        """.format(days)
        
        agg = pl.read_database(query, self.conn, parameters=[symbols])
        
        scores = (agg
                 .with_columns([
                     (1 / pl.col("avg_error").clip(1e-6)).alias("accuracy"),
                     (pl.col("total_predictions") * pl.col("diversity")).alias("breadth"),
                     (1 / pl.col("avg_error").clip(1e-6) * 
                      pl.col("total_predictions") * pl.col("diversity")).alias("skill_score")
                 ])
                 .select([
                     "analyst_name", "skill_score", "accuracy", "avg_error", 
                     "total_predictions", "diversity"
                 ])
                 .sort("skill_score", descending=True)
                 .with_columns(pl.now().alias("updated_at"))
                 .head(50)
        )
        
        self.update_analyst_scores(scores.to_dicts())
        return scores

    def get_stats(self) -> Dict[str, Any]:
        """System statistics."""
        stats = {
            "total_analysts": self.conn.execute("SELECT COUNT(*) FROM analyst_scores").fetchone()[0],
            "total_targets": self.conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0],
            "database_size_mb": self.db_path.stat().st_size / (1024*1024) if self.db_path.exists() else 0
        }
        return stats

    def backup(self, backup_path: str):
        """Create database backup."""
        self.db_path.replace(backup_path)
        logger.info(f"Backup created: {backup_path}")

# Global instance
db = Database()

if __name__ == "__main__":
    # Test queries
    print("Top 10 analysts:")
    print(db.get_top_analysts())
    
    print("\nSystem stats:")
    print(db.get_stats())
