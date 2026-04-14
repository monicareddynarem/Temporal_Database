# High-Performance Temporal Database (PostgreSQL)

## Overview

This project implements a temporal database system designed to handle high-frequency financial tick data using **PostgreSQL**.

The system focuses on three core challenges:

* high-throughput data ingestion
* efficient analytical querying
* scalable long-term storage

The objective is to evaluate how far a standard relational database can be optimized to support time-series workloads without relying on specialized time-series engines.

---

## System Design

The architecture consists of the following components:

```text
Data Generators → Ingestion Pipeline → Raw Tick Storage
                         ↓
              Aggregation Engine
                         ↓
        OHLCV Tables (1s, 1m resolution)
                         ↓
               Visualization Layer
```

Key design strategies include:

* batch-based ingestion using bulk operations
* hierarchical aggregation (ticks → 1s → 1m)
* partitioned storage for scalability
* compression techniques for reducing storage overhead


## Key Features

### 1. Vectorized Ingestion Pipelines

Multiple ingestion strategies are implemented and benchmarked, including:

* row-wise insertion
* batch list insertion
* NumPy-based vectorized insertion
* bulk ingestion using COPY

Vectorized ingestion combined with `COPY` enables significantly higher throughput compared to naive insertion approaches.


### 2. Continuous Background Aggregation

A background process (`genaggregate.py`) continuously aggregates raw tick data into:

```text
ohlcv_1s
ohlcv_1m
```

This reduces query complexity and improves analytical query performance by operating on pre-aggregated data.


### 3. Compressed Storage using Array Bucketing

To manage storage growth, the system implements a compressed schema:

* ticks are grouped by time buckets and symbol
* price and volume data are stored as arrays
* compression techniques (e.g., LZ4) are applied

This approach reduces storage requirements while maintaining query efficiency.


### 4. Interactive Dashboard

A frontend interface built using **Streamlit** provides:

* candlestick chart visualization
* moving average analysis
* system performance monitoring


## Project Structure

```text
TEMPORAL_DATABASE/
├── benchmarks/           
│   ├── index_vs_noindex.py   
│   ├── ingestion_compare.py  
│   ├── memory_analysis.py    
│   └── query_latency.py      
│
├── data_srcs/            
│   ├── live_gen.py           
│   ├── mock_gen.py           
│   └── nse_gen.py            
│
├── ingestion/            
│   ├── row_wise_ingester.py      
│   ├── batch_list_ingester.py    
│   ├── batch_numpy_ingester.py   
│   └── compressed_ingester.py    
│
├── schemas/              
│   ├── baseline.sql              
│   ├── indexed.sql               
│   └── compressed_indexed.sql    
│
├── utils/                
├── app.py                
├── genaggregate.py       
└── run_pipeline.py       
```

## Execution Workflow

### Step 1: Initialize Database

Load the required schema:

```bash
psql -U postgres -d postgres -f schemas/compressed_indexed.sql
```


### Step 2: Start Background Processes

Terminal 1:

```bash
python genaggregate.py
```

Terminal 2:

```bash
streamlit run app.py
```

### Step 3: Run Ingestion Pipeline

Terminal 3:

```bash
python run_pipeline.py
```

Select the ingestion mode corresponding to the loaded schema.


## Benchmark Summary

The system includes a benchmarking suite to evaluate performance.

### Ingestion Performance

* Bulk `COPY` ingestion achieves significantly higher throughput compared to row-wise insertion
* Suitable for high-frequency data streams


### Query Performance

* Queries on pre-aggregated OHLCV tables are substantially faster than raw tick queries
* Reduces latency for analytical workloads


### Storage Optimization

* Array-based compression reduces storage footprint
* Enables long-term retention of time-series data


## Conclusion

This project demonstrates how a relational database system can be extended to support time-series workloads through:

* optimized ingestion pipelines
* hierarchical aggregation
* efficient storage design

It serves as a practical implementation of temporal data management concepts in financial systems.