# Day 6 – Medallion Architecture 🥉⚪🟡  
**Databricks 14-Day AI & Data Engineering Challenge**  
 
---

## 📌 Overview
On **Day 6**, I focused on implementing the **Medallion Architecture**, a key data engineering pattern used in modern **lakehouse systems**.  

The goal was to understand the **Bronze → Silver → Gold layers**, learn best practices for each, and apply **incremental processing patterns** using Spark and Delta Lake.

---

## 🏗️ Medallion Architecture
Source Systems
↓
Bronze Layer (Raw)
↓
Silver Layer (Cleaned & Validated)
↓
Gold Layer (Business Aggregates)


Each layer has a distinct role:  
- **Bronze:** Raw ingestion, preserves everything  
- **Silver:** Cleansed, trusted data  
- **Gold:** Aggregated, business-ready metrics  

---

## 🥉 Bronze Layer – Raw Data

**Purpose:** Capture data exactly as received.

**Best Practices:**
- Append-only ingestion  
- No business logic  
- Preserve raw payload  
- Allow schema evolution  

**What I Did:**
- Ingested raw events from source  
- Stored ingestion metadata (`ingestion_time`, `ingestion_date`)  
- Preserved original schema and values  

---

## ⚪ Silver Layer – Cleaned & Validated

**Purpose:** Ensure **trusted, queryable data**.

**Best Practices:**
- Explicit schema enforcement  
- Data quality checks  
- Deduplication  
- Safe type casting (`try_cast`)  
- Enrich with derived columns  

**What I Learned & Applied:**
- Cast `price` safely with `try_cast` to handle malformed data  
- Filtered invalid records (nulls, out-of-range values)  
- Deduplicated events (`user_session` + `event_time`)  
- Derived new columns (`event_date`, `product_name`, `price_tier`)  
- Created a stable Silver schema contract for downstream consumption  

---

## 🟡 Gold Layer – Business Aggregates

**Purpose:** Provide **analytics-ready, business-focused data**.

**Best Practices:**
- Aggregate metrics (revenue, purchases, conversion rate)  
- Safe calculations (avoid division by zero)  
- Depend only on Silver layer  
- Maintain a stable schema  

**What I Built:**
- Product-level performance metrics  
- Total revenue and events per product  
- Conversion rate metrics  
- Safe Gold tables for BI and analytics  

---

## 🔁 Incremental Processing Patterns

**Concepts Learned:**
- Reprocessing everything is inefficient  
- Incremental pipelines are critical for production  

**Patterns Covered:**
- Timestamp-based filtering  
- MERGE for upserts / Slowly Changing Dimensions  
- Partition-based refresh for aggregates  

---

## 🧠 Key Takeaways

> **Bronze captures reality.  
> Silver enforces truth.  
> Gold delivers insight.**

Separation of responsibilities across layers ensures:
- Reliability  
- Trust in analytics  
- Easier debugging and maintenance  

---

## 🛠️ Tasks Completed

1. Designed a 3-layer architecture  
2. Built **Bronze layer**: raw ingestion with metadata  
3. Built **Silver layer**: cleaning, deduplication, type safety, enrichment  
4. Built **Gold layer**: business aggregates with safe metrics  

---

## 🔗 Resources

- 📘 [Medallion Architecture – Databricks](https://www.databricks.com/glossary/medallion-architecture)  
- 🎥 [Medallion Architecture Explained](https://www.youtube.com/watch?v=njjBdmAQnR0)  
- 🛠️ [Build a Medallion Architecture with Databricks](https://youtu.be/yy9H4mlOG6I?si=cuo_wiQtW0XpbbYU)  
- 🔁 [Incremental Processing Patterns](https://youtu.be/GjV2m8b9fNY?si=-3g7rbtbj9i3axJG)  

---

## 🏷️ Tags / Keywords

`#DatabricksWithIDC #Databricks #Codebasics #MedallionArchitecture #BronzeSilverGold #ETL`

---




