# Databricks notebook source
# MAGIC %pip install transformers torch
# MAGIC

# COMMAND ----------

from transformers import pipeline

# Load pre-trained sentiment model
classifier = pipeline("sentiment-analysis")

# Sample text (you can use dummy data)
reviews = [
    "This product is amazing!",
    "Terrible quality, waste of money"
]

# Run inference
results = classifier(reviews)
results


# COMMAND ----------

import mlflow

with mlflow.start_run(run_name="sentiment_analysis_demo"):
    mlflow.log_param("model", "pretrained_sentiment_model")
    mlflow.log_metric("sample_records", len(reviews))
