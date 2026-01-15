# Day 7 – Workflows & Job Orchestration (Databricks)

## 📌 Overview
Day 7 focused on turning individual Databricks notebooks into **production-ready, automated data pipelines** using Databricks Workflows. The goal was to understand how orchestration, parameterization, scheduling, and dependencies work together in real-world data engineering.

---

## 📚 What I Learned

### 🔹 Databricks Jobs vs Notebooks
- Notebooks are ideal for development and exploration
- Databricks Jobs are designed for automation, scheduling, monitoring, and production execution

### 🔹 Multi-Task Workflows
- Built workflows with multiple tasks
- Modeled real-world pipelines using the **Bronze → Silver → Gold** architecture

### 🔹 Parameters & Scheduling
- Used notebook widgets to pass parameters dynamically
- Scheduled workflows to run automatically without manual intervention

### 🔹 Error Handling & Dependencies
- Defined task dependencies to control execution order
- Understood how failures prevent downstream tasks from running

---

## 🛠️ Tasks Completed

- Added **parameter widgets** to Databricks notebooks  
- Created a **multi-task job workflow** (Bronze → Silver → Gold)  
- Configured **task dependencies** between pipeline stages  
- Scheduled the job for automated execution  

---

## 🔑 Key Takeaways

- Data engineering goes beyond writing Spark code
- Orchestration and automation are critical for production pipelines
- Databricks Workflows simplify building scalable, reliable ETL processes

---

## 🚀 Tools & Concepts Used
- Databricks Workflows
- Databricks Jobs
- Notebook Parameters (Widgets)
- Task Dependencies & Scheduling
- Medallion Architecture (Bronze, Silver, Gold)

---

## 📣 Mentions
@Indian Data Club  
@Databricks  
@Codebasics  

---

## 🏷️ Tags
#IndianDataClub #Databricks #Codebasics #DatabricksWithIDC #DataEngineering #ETL #Lakehouse
