## 📅 Day 5 – Delta Lake Advanced  
**Databricks 14-Days AI Challenge**

### 📌 Overview
Day 5 focused on advanced Delta Lake capabilities that enable reliable data management, efficient updates, performance optimization, and data lifecycle control in Databricks.

---

## 📚 Topics Covered

### 🔹 Time Travel (Version History)
- Query previous versions of Delta tables
- Debug incorrect updates and restore historical data
- Supports both version-based and timestamp-based queries

### 🔹 MERGE Operations (Upserts)
- Incremental data processing using `MERGE INTO`
- Handles updates and inserts in a single transaction
- Ensures ACID compliance for streaming and batch workloads

### 🔹 OPTIMIZE & ZORDER
- Compacts small files to improve read performance
- ZORDER improves data skipping for frequently filtered columns
- Enhances query speed and resource efficiency

### 🔹 VACUUM (Cleanup)
- Removes obsolete files no longer referenced by Delta logs
- Helps control storage costs
- Retention policies ensure data safety

---

## 🛠️ Hands-on Tasks Completed
- ✅ Implemented incremental **MERGE (upsert)** logic  
- ✅ Queried **historical table versions** using Time Travel  
- ✅ Optimized Delta tables using **OPTIMIZE & ZORDER**  
- ✅ Cleaned up unused data files using **VACUUM**

---

## 🎯 Key Takeaways
- Delta Lake provides **ACID transactions** on data lakes
- Time Travel enables **safe debugging and data recovery**
- Proper optimization significantly improves performance
- Regular cleanup is essential for efficient storage management

---

---
### 🏷️ Tags
`DatabricksWithIDC` `Databricks` `IndianDataClub` `DataEngineering` `CodeBasics` 
