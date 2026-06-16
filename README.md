# 🛸 ORBITAL — Satellite Tracking Pipeline

Real-time tracking pipeline for satellites and space debris using Kafka, DuckDB, Airflow, and Streamlit.

## 📌 Use Case

Over 10,000 objects orbit Earth — active satellites, space stations, and debris from past collisions. This pipeline tracks their positions in real time, stores them in a structured data store, and visualises them on an interactive 3D globe.

## 🏗️ Architecture

CelesTrak TLE Data

↓

ETL Batch (Python)

↓

DuckDB (raw_satellites)

↓

Kafka Producer (Skyfield → positions every 10s)

↓

Kafka Topic: satellite_positions

↓

Kafka Consumer (writes to DuckDB)

↓

DuckDB (satellite_positions)

↓

Airflow DAG (orchestration, refresh views, cleanup)

↓

Streamlit Dashboard (5 live visualisations)

## 🧱 Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Skyfield, CelesTrak |
| Streaming | Apache Kafka |
| Storage | DuckDB |
| Transformation | SQL views (ELT) |
| Orchestration | Apache Airflow |
| Visualisation | Streamlit, Plotly |
| Infrastructure | Docker Compose |

## 📁 Project Structure

satellite-pipeline/

├── docker-compose.yml       # Kafka + Zookeeper + Airflow

├── requirements.txt

├── etl/

│   └── fetch_tle.py         # ETL batch : fetch satellite data

├── kafka/

│   ├── producer.py          # Kafka producer : positions every 10s

│   └── consumer.py          # Kafka consumer : writes to DuckDB

├── airflow/

│   └── dags/

│       └── satellite_dag.py # Airflow DAG

├── dashboard/

│   └── app.py               # Streamlit dashboard

└── db/                      # DuckDB database (auto-generated)

## 🚀 How to Run

### 1. Prerequisites
- Docker Desktop running
- Python 3.11+ with virtual environment

### 2. Install dependencies
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Start Kafka
```bash
docker-compose up -d zookeeper kafka
```

### 4. Run ETL batch
```bash
python etl/fetch_tle.py
```

### 5. Start Kafka producer
```bash
python kafka/producer.py
```

### 6. Start Kafka consumer (new terminal)
```bash
python kafka/consumer.py
```

### 7. Launch dashboard (new terminal)
```bash
streamlit run dashboard/app.py
```

Dashboard available at: http://localhost:8501

## 📊 Dashboard

5 live visualisations refreshed every 10 seconds:
1. 🌍 Interactive 3D globe with orbital trajectories
2. 📈 Latitude over time per satellite
3. 📊 Latitude distribution histogram
4. 📋 Statistics table per satellite
5. 🥧 Object type distribution (stations / active / debris)

## 👩‍💻 Author

Angelikia Kavuansiko - MSc Computer Science & Data Science, ESILV Paris (2025–2027)  
ETL & Pipeline Orchestration - Prof. Murali Krishna MOPIDEVI