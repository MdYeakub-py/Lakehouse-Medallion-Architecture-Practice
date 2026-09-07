# Lakehouse-Medallion-Architecture-Practice

# Sales Lakehouse — Medallion Architecture on Microsoft Fabric

End-to-end data engineering pipeline built on **Microsoft Fabric**, transforming raw food-delivery transactional data into analytics-ready star-schema tables using the **Bronze → Silver → Gold medallion architecture**, orchestrated with **Fabric Data Pipelines** and consumed through a **Power BI semantic model**.

---

## 📌 Project Overview

This project simulates a real-world food-delivery analytics platform, covering the full data engineering lifecycle:

- Ingesting raw operational data (Users, Restaurants, Orders)
- Cleaning and standardizing it with **PySpark**
- Modeling it into a **star schema** for reporting
- Automating the pipeline with **scheduled orchestration**
- Exposing it through a **Power BI semantic model** with DAX measures

It was built as a hands-on practice project while preparing for the **Microsoft Certified: Fabric Data Engineer Associate (DP-700)** certification, applying concepts directly to an end-to-end scenario rather than isolated exercises.

---

## 🏗️ Architecture


<img width="3456" height="296" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/647958a9-e0d5-4a46-b88e-5225738a0b57" />





```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│   Raw CSVs   │ ──▶ │   Bronze Layer   │ ──▶ │   Silver Layer   │ ──▶ │     Gold Layer      │
│ Users, Rest, │     │  (raw ingestion, │     │  (cleaned, typed,│     │ (star schema +      │
│   Orders     │     │   as-is Delta)   │     │   deduplicated)  │     │  business aggregates)│
└─────────────┘     └──────────────────┘     └──────────────────┘     └──────────┬──────────┘
                                                                                    │
                                                                                    ▼
                                                                        ┌────────────────────┐
                                                                        │  Semantic Model     │
                                                                        │  (star schema,      │
                                                                        │   DAX measures)     │
                                                                        └──────────┬──────────┘
                                                                                    │
                                                                                    ▼
                                                                        ┌────────────────────┐
                                                                        │    Power BI Report  │
                                                                        └────────────────────┘
```


Orchestration: a **Fabric Data Pipeline** (`Sales_Medallion_Pipeline`) chains the Silver and Gold notebooks sequentially with a scheduled trigger.

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Storage | OneLake, Delta Lake |
| Compute | Microsoft Fabric Spark (PySpark) |
| Orchestration | Fabric Data Pipelines (scheduled triggers) |
| Modeling | Fabric Semantic Model (star schema, Direct Lake) |
| Reporting | Power BI, DAX |

---

## 📂 Repository Structure

```
├── notebooks/
│   ├── 01_silver_layer.py     # Bronze → Silver: cleaning, typing, deduplication
│   └── 02_gold_layer.py       # Silver → Gold: star schema + aggregate tables
├── docs/
│   ├── architecture.png       # Architecture diagram
│   ├── data_dictionary.md     # Column-level documentation per table
│   └── screenshots/           # Pipeline runs, semantic model, report visuals
├── sample_data/
│   └── *.csv                  # Small sample of source data (schema reference only)
└── README.md
```

---

## 🥉 Bronze Layer

Raw source data landed as-is into Delta tables, with no transformation:

- `bronze_users` — customer demographic data
- `bronze_restaurant` — restaurant listings, ratings, cuisines
- `bronze_orders` — transactional order records

---

## 🥈 Silver Layer — Data Cleaning

Key cleaning operations applied via PySpark (`01_silver_layer.py`):

- **Type correction** — `order_date` cast to date, `Restaurant_id`/`Sales_QTY` cast to integer, `Sales_amount` cast to double
- **Null/placeholder handling** — `Rating` values stored as `"--"` converted to proper nulls
- **Categorical standardization** — trimmed whitespace, corrected inconsistent labels (e.g. `"Self Employeed"` → `"Self Employed"`)
- **Denormalization fix** — comma-separated `Cuisine` values exploded into a dedicated `bridge_restaurant_cuisine` table for proper many-to-many modeling
- **Data quality flagging** — orphaned foreign keys (orders referencing non-existent restaurants), missing restaurant references, and negative sale amounts flagged via boolean columns rather than silently dropped, preserving auditability
- **Deduplication** on primary keys for both dimension tables

---

## 🥇 Gold Layer — Star Schema & Business Tables

Built via `02_gold_layer.py`:

**Star schema:**
- `fact_orders` — order-grain fact table with surrogate `DateKey`
- `dim_users`, `dim_restaurant`, `dim_date`, `bridge_restaurant_cuisine`

**Business-ready aggregate tables** (for quick reference / non-interactive reporting):
- `monthly_sales_summary`
- `restaurant_performance`
- `user_segment_sales`
- `cuisine_sales`

Rows flagged as low-quality in Silver (orphaned or missing restaurant references, negative sale amounts) are filtered out before the Gold layer, since Gold is the reporting-ready layer.

---

## ⚙️ Orchestration & Scheduling

- A **Fabric Data Pipeline** chains `Run_Silver_Layer` → `Run_Gold_Layer` sequentially
- A **scheduled trigger** runs the pipeline automatically on a recurring basis
- A **Wait activity** is inserted between the two notebook activities to allow the prior Spark session to fully release compute before the next one starts

---

## 📊 Semantic Model & Reporting

A Fabric **Semantic Model** was built directly on top of the Gold layer using **Direct Lake** mode:

- Star-schema relationships: `fact_orders` ↔ `dim_users`, `dim_restaurant`, `dim_date` (via surrogate key), and `bridge_restaurant_cuisine` ↔ `dim_restaurant`
- `dim_date` marked as the model's official Date Table to enable time-intelligence DAX
- Core DAX measures (`Total Sales`, `Total Orders`, `Distinct Users`, etc.) built directly on `fact_orders`, rather than relying on the static Gold aggregate tables — keeping the model fully dynamic and slicer-responsive

---

## 🧩 Challenges & Solutions

| Challenge | Solution |
|---|---|
| `TooManyRequestsForCapacity` error during scheduled runs | Diagnosed as Spark session overlap caused by an overly frequent (5-minute) trigger interval; corrected the schedule to a daily/weekly cadence and added a `Wait` activity between chained notebook runs to let compute fully release |
| Comma-separated `Cuisine` values inflating dimension complexity | Normalized into a separate bridge table for proper many-to-many relationship modeling |
| Static aggregate tables (`monthly_sales_summary`, etc.) not fitting cleanly into a star schema | Kept them out of the semantic model relationships; replaced with dynamic DAX measures on the fact table for full interactivity |

---

## 🎯 Skills Demonstrated

- Medallion architecture design (Bronze/Silver/Gold)
- PySpark data cleaning, type standardization, and data quality flagging
- Delta Lake table management
- Star schema and dimensional modeling
- Fabric pipeline orchestration, scheduling, and capacity troubleshooting
- Power BI semantic modeling (Direct Lake) and DAX measure design

---

## ▶️ How to Reproduce

1. Create a Fabric Lakehouse and load the source CSVs into Bronze Delta tables
2. Run `notebooks/01_silver_layer.py` to produce the Silver layer
3. Run `notebooks/02_gold_layer.py` to produce the Gold layer and star schema
4. Build a Semantic Model on the Gold Lakehouse and define the relationships described above
5. Connect Power BI and build reports on top of the semantic model

---


---

## 👤 Author

**Md Yeakub** — Data Analyst | Power BI Developer | Aspiring Data Engineer
