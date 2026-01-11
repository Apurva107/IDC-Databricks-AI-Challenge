## 📅 Day 2 – Apache Spark Fundamentals

### 📖 Overview
Day 2 focused on understanding the core concepts of Apache Spark and how data is processed at scale. The emphasis was on learning Spark’s execution model and applying fundamental DataFrame operations using PySpark.

---

### 🧠 What I Learned

- **Spark Architecture (Driver, Executors, DAG):**  
  Learned how the driver builds execution plans as DAGs and distributes tasks to executors for parallel processing.

- **DataFrames vs RDDs:**  
  Understood why DataFrames are preferred due to built-in optimizations, schema enforcement, and better performance.

- **Lazy Evaluation:**  
  Learned that Spark delays execution of transformations until an action is triggered, allowing it to optimize the entire workflow.

- **Notebook Magic Commands (`%python`, `%sql`, `%fs`):**  
  Practiced switching between PySpark, Spark SQL, and file system operations within the same notebook environment.

---

### 🛠️ Hands-on Work
- Uploaded and read a sample e-commerce CSV dataset
- Created Spark DataFrames with inferred schema
- Performed core transformations:
  - Column selection
  - Row filtering
  - Grouping and aggregations
  - Sorting and limiting results
- Analyzed event- and brand-level metrics
- Exported aggregated results for further use

---

### 🔑 Key Takeaways
- Spark transformations are lazy and do not execute immediately
- Actions trigger DAG creation and job execution
- DataFrames are optimized and preferred over RDDs for most use cases
- Understanding Spark internals helps write more efficient data pipelines


---

### 📚 Learning References
- [Databricks PySpark Guide](https://docs.databricks.com/aws/en/pyspark)  
- [Apache Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---

### 🙏 Acknowledgments
Special thanks to:  
- #DatabricksWithIDC  
- @IndianDataClub  
- @Codebasics  
- @Databricks  

For this challenge.


