Initial commit: Airbnb project structure with Medallion Architecture

# Airbnb Data Analysis with Medallion Architecture

**Objective:** Analyze Airbnb listings using Databricks and Medallion Architecture to provide insights.

---

## Tools & Technologies
- Databricks (PySpark, Delta Lake)
- Medallion Architecture (Bronze → Silver → Gold)
- Power BI (Dashboards)
- Python
- SQL

---

## Medallion Architecture
- **Bronze Layer:** Raw CSVs ingested into Databricks  
- **Silver Layer:** Cleaned and transformed data (removing nulls, formatting dates, numeric conversions)  
- **Gold Layer:** Aggregated tables for analysis and dashboards  

---

## Project Workflow
1. Ingested Airbnb listing data into **Bronze tables**  
2. Cleaned and transformed data → **Silver tables**  
3. Aggregated and summarized data → **Gold tables**  
4. Built **Power BI dashboards** for data visualization to understand key metrics:
   - Calendar
   - Listings Summary
   - Reviews
   - Location

---

## Project Artifacts
- `notebooks/` – PySpark notebooks for ETL and data transformations  
- `medallion_architecture/` – Bronze, Silver, Gold tables (sample data)  
- `dashboards/` – Power BI dashboards and screenshots  
- `images/` – Charts and Medallion architecture diagram  

---

## Contact
- [LinkedIn]((https://www.linkedin.com/in/celes-anusha-joseph-8237532b9/))  
