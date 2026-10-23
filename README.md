<div align="center">

<img src="assets/readme/hero.svg" width="100%" alt="CASSANDRA / RESEARCH OPERATIONS engineering overview"/>

**Process supervision; Lite-Invest market collector; order book features; Flask/SocketIO dashboard**

</div>

# CASSANDRA / RESEARCH OPERATIONS

## Original research direction

MAIN.py implements Config, ManagedComponent and SystemManager using multiprocessing, status monitoring and shutdown. Collector includes Tick/Candle/Cluster/OrderBook dataclasses, CandleBuilder, FeatureEngineer, OrderBookProcessor, LabelGenerator and CSV/Parquet/NPZ/JSONL exports. The order tester implements price conversion, state transitions, broker requests, stop-limit management and Flask/SocketIO events.

## Implemented engineering and algorithmic value

The repository currently contains credentials embedded in Python source; revoke and replace these before any public deployment. The source may place real orders, so descriptive analysis is not permission to execute them.

## Scientific and mathematical interpretation

Separate read-only data collector and explicitly authorized order executor; add authenticated event streaming, credential hygiene, demo fixtures, end-to-end tests and immutable trade reports.

## Research and application opportunities

undefined

## Evidence context and development maturity

For each price level l, order-book imbalance may be represented as I_l=(B_l-A_l)/(B_l+A_l) when depth is positive. This is a recommended derived feature, not evidence that every exported field uses this exact formula. Tick cumulative delta is signed buy volume minus sell volume.

## Source navigation

- [`MAIN.py`](MAIN.py)
- [`cassandra_ai_collector.py`](cassandra_ai_collector.py)
- [`cassandra_v6_FIXED.py`](cassandra_v6_FIXED.py)
- [`dashboard.html`](dashboard.html)

## Publication protocol

Claims must refer to the exact current source commit or an identified experiment artifact. Source code establishes implementation; experiments require frozen inputs, complete costs, successful repeated checks and a labelled evaluation regime. Financial values are attributed to their measurement environment. This page is a scientifically grounded overview and is not an offer of investment returns.
