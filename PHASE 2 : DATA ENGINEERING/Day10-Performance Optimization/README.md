# 📅 Day 10 – Performance Optimization

## 📌 Overview
Day 10 focused on improving query performance in Databricks by understanding how Spark executes queries and how Delta Lake optimization techniques impact speed and efficiency. The goal was to move beyond writing queries and start thinking about data layout and execution behavior.

---

## 📘 What I Learned

- **Query Execution Plans**  
  Learned how to analyze execution plans to understand how Spark reads data, applies filters, and identifies performance bottlenecks.

- **Partitioning Strategies**  
  Understood how partitioning large datasets on frequently filtered columns helps reduce data scanned through partition pruning.

- **OPTIMIZE**  
  Learned how OPTIMIZE compacts small files in Delta tables, improving read performance and reducing overhead.

- **ZORDER**  
  Learned how ZORDER reorganizes data to enable efficient data skipping, especially for selective queries.

- **Caching Techniques**  
  Understood when caching is useful for speeding up repeated and iterative queries.

---

## 🛠️ Tasks Performed

- Analyzed query execution plans to identify inefficiencies
- Created a partitioned Delta table for transactional data
- Applied OPTIMIZE to compact files
- Applied ZORDER on high-cardinality columns for faster lookups
- Benchmarked query performance before and after optimization
- Verified partition pruning and data skipping through execution plans

---

## 🔑 Key Takeaways

- Query performance depends heavily on how data is stored and organized
- Partitioning enables efficient data access and pruning
- OPTIMIZE and ZORDER significantly improve query efficiency
- Execution plans are essential for validating performance improvements
- Performance tuning is a critical skill for real-world data engineering

---

## 🚀 Outcome
Successfully optimized Delta tables and observed improved query performance by applying Databricks best practices for partitioning, file compaction, and data layout.

---

## 🙌 Acknowledgements

- @Indian Data Club
- @Databricks
- @Codebasics
- #DatabricksWithIDC
