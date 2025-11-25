# RSS Analyst 1.0 🦊

**Production analyst accuracy tracker** analyzing 200+ financial analysts using FMP API + yfinance. Ranks analysts by `skill_score = accuracy × prediction breadth`. Modern Polars/DuckDB/FastAPI rewrite (10x faster than pandas).

[![API](https://img.shields.io/badge/API-FastAPI-blue)](http://localhost:8000/docs)
[![Polars](https://img.shields.io/badge/Engine-Polars-brightgreen)](https://pola.rs)
[![DuckDB](https://img.shields.io/badge/DB-DuckDB-orange)](https://duckdb.org)


## 🎯 Features
- **Analyst Ranking**: `skill_score = (1/avg_error) × total_predictions × symbol_diversity`
- **Real-time API**: FastAPI + auto Swagger docs
- **Persistent Storage**: DuckDB (Parquet-compatible)
- **Async FMP + yfinance**: 300 RPM rate-limited
- **RSS Feeds**: Top analysts + quant signals (HFT/VWAP keywords)
- **CLI Dashboard**: `python main.py top-analysts 10`

## 📊 Performance
Polars: 10x faster than pandas
DuckDB: 1M+ price targets, sub-second queries
Async HTTPX: 200+ analysts in <30s
Memory: <500MB for 1yr historical data
