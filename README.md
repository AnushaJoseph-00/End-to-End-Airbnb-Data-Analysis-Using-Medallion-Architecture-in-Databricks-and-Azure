# Airbnb Data Pipeline on Databricks using Medallion Architecture

An end-to-end data pipeline that ingests raw Airbnb listing data into Databricks, refines it through **Bronze → Silver → Gold** layers using PySpark and Delta Lake, and serves analysis-ready tables to Power BI dashboards.

---

## Architecture

```
 Raw CSVs  ──►  BRONZE  ──►  SILVER  ──►  GOLD  ──►  Power BI
 (Airbnb)       raw,         cleaned,      aggregated   dashboards
                as-ingested  typed,        business
                             validated     metrics
```

![Medallion Architecture](images/medallion_architecture.png)

| Layer | Purpose | Key operations |
|---|---|---|
| **Bronze** | Raw data, preserved as ingested | CSV ingestion into Delta tables |
| **Silver** | Clean, consistent, typed data | Null handling, date formatting, numeric type conversions |
| **Gold** | Business-ready aggregates | Summary tables for calendar, listings, reviews and location |

---

## Tech Stack

- **Databricks** — PySpark, Delta Lake
- **Python** and **SQL** — transformations and queries
- **Power BI** — dashboards and visualisation

---

## Dataset

- **Source:** Airbnb listing data (listings, calendar, reviews)
- **City / snapshot:** _<add city and snapshot date>_
- **Size:** _<add approx. row counts>_

---

## Pipeline Workflow

1. **Ingest** — load raw Airbnb CSVs into Bronze Delta tables
2. **Clean & transform** — remove nulls, standardise dates and convert numeric fields into Silver tables
3. **Aggregate** — build summarised Gold tables for analysis
4. **Visualise** — connect Power BI to the Gold tables for dashboards covering:
   - Calendar (availability and pricing over time)
   - Listings summary
   - Reviews
   - Location

---

## Dashboards

![Dashboard](dashboards/dashboard_screenshot.png)

### Key Findings

- _<insight 1>_
- _<insight 2>_
- _<insight 3>_

---

## Repository Structure

```
.
├── notebooks/                 # PySpark notebooks for ETL and transformations
├── medallion_architecture/    # Sample Bronze, Silver and Gold tables
├── dashboards/                # Power BI files and screenshots
├── images/                    # Charts and architecture diagram
└── README.md
```

---

## How to Run

1. Import the notebooks from `notebooks/` into a Databricks workspace
2. Upload the raw Airbnb CSVs and update the file paths in the Bronze notebook
3. Run the notebooks in order: Bronze → Silver → Gold
4. Open the Power BI file in `dashboards/` and connect it to the Gold tables

---

