# 📅 Day 4 – Delta Lake Introduction

## 📌 Overview
On Day 4, I explored **Delta Lake** and how it enhances data lakes by adding **reliability, consistency, and transactional guarantees** on top of Parquet files using **Apache Spark**.  

The focus was on **understanding core concepts** and learning how Delta Lake differs from traditional Parquet.

---

## 🎯 Topics Covered

- **Delta Lake** – A storage layer that adds **ACID transactions, schema enforcement, and time travel** on top of Parquet files, making data lakes production-ready.  
- **ACID Transactions** – Ensures **atomic, consistent, isolated, and durable** writes, preventing partial or corrupt data.  
- **Schema Enforcement** – Validates incoming data against the table schema and **prevents bad data** from being written.  
- **Delta vs Parquet** – Parquet is a columnar file format optimized for analytics, but **Delta Lake builds on Parquet** to add **transactions, updates/deletes, schema enforcement, and time travel**.  
- **Creating Delta Tables** – How to create **managed tables** (Spark controls storage) and **external tables** (pointing to a file path) using PySpark or SQL.  
- **Converting CSV to Delta** – Understanding how raw CSV data can be converted into Delta format for **reliable, queryable storage**.  
- **Handling Duplicates** – Techniques to prevent duplicate records and safely perform incremental data loads.  
- **Initial vs Incremental Loads** – First load uses **overwrite**; subsequent loads use **append** or **MERGE** for incremental updates.

---

## 🛠️ Tasks Performed

- Converted **CSV datasets into Delta format**  
- Created **managed and external Delta tables**  
- Tested **schema enforcement** to prevent incompatible data writes  
- Compared **Delta Lake and Parquet** features  
- Identified and handled **duplicate records** during incremental loads  
- Performed **initial data load** and planned for **incremental data ingestion**

---

## 🔁 Delta Lake vs Parquet

| Feature | Parquet | Delta Lake |
|---------|--------|------------|
| Columnar storage | ✅ | ✅ |
| ACID transactions | ❌ | ✅ |
| Schema enforcement | ❌ | ✅ |
| Updates / Deletes | ❌ | ✅ |
| Time travel | ❌ | ✅ |

---


## 🏷️ Tags
`#Databricks`   
`#DatabricksWithIDC`   
`#Codebasics` `#PySpark` 

---

## 🔗 Mentions 
- @IndianDataClub  
- @Databricks  
- Codebasics  


---

📌 **Day 4 Status:** ✅ Completed
