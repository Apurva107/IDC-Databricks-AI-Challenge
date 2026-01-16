# 📅 Day 8 – Data Governance with Unity Catalog (Databricks)

## 📘 Overview
Day 8 focused on **data governance and security in Databricks using Unity Catalog**.  
The goal was to understand how modern data platforms organize data, control access, and track data lineage to ensure security, compliance, and trust.

---

## 🎯 What I Learned

### 1️⃣ Catalog → Schema → Table Hierarchy
- Unity Catalog uses a **three-level namespace** to organize data
- Improves data discoverability, governance, and scalability


---

### 2️⃣ Access Control (GRANT / REVOKE)
- Implemented fine-grained permissions at:
  - Catalog level
  - Schema level
  - Table and View level
- Followed the **principle of least privilege**

---

### 3️⃣ Managed vs External Tables
- **Managed Tables**
  - Storage and lifecycle handled by Databricks
  - Dropping the table removes the underlying data
- **External Tables**
  - Data stored in external cloud storage (S3 / ADLS / GCS)
  - Dropping the table does not delete the data

---

### 4️⃣ Data Lineage
- Learned how Unity Catalog automatically captures:
  - Upstream data sources
  - Downstream consumers
  - Table and column-level lineage
- Helps with impact analysis and debugging

---

## 🛠️ Tasks Performed

- Created **catalogs and schemas** for structured data organization
- Registered **Delta tables** (managed and external)
- Configured **role-based access control** using GRANT and REVOKE
- Created **secure views** to provide controlled data access
- Verified **data lineage** across tables and views

---

## 🔑 Key Takeaways
- Data governance is a core component of modern data engineering
- Unity Catalog simplifies security, access control, and lineage management
- Views are effective for secure data sharing
- Proper structure improves maintainability and scalability

---

## 🚀 Tools & Technologies
- Databricks
- Unity Catalog
- Delta Lake
- SQL

---

## 🙌 Acknowledgements
This learning challenge is inspired by and guided by:

- **Indian Data Club**  
- **Databricks**  
- **codebasics**  


---

📚 *Day 8 completed — strengthening foundations in data governance and security.*
