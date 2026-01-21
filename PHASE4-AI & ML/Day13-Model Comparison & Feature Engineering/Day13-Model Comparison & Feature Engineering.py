# Databricks notebook source
import mlflow
import mlflow.sklearn

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import r2_score

# Load data
df = spark.table("workspace.default.gold_product").toPandas()

X = df[["unique_views", "unique_purchases", "conversion_rate"]]
y = df["total_revenue"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# COMMAND ----------

df.isna().sum()


# COMMAND ----------

df = df.copy()
df["total_revenue"] = df["total_revenue"].fillna(0)
df["conversion_rate"] = df["conversion_rate"].fillna(0)


# COMMAND ----------

X = df[["unique_views", "unique_purchases", "conversion_rate"]]
y = df["total_revenue"]


# COMMAND ----------

print(X.isna().sum())
print(y.isna().sum())


# COMMAND ----------

def model_run(model):
    model.fit(X_train, y_train)
    return model.score(X_test, y_test)


# COMMAND ----------

def model_run(model):
    print("NaN in y_train:", y_train.isna().sum())
    print("NaN in X_train:", X_train.isna().sum().sum())
    model.fit(X_train, y_train)
    return model.score(X_test, y_test)


# COMMAND ----------

spark_df = spark.table("workspace.default.gold_product")


# COMMAND ----------

spark_df = spark_df.fillna({
    "total_revenue": 0,
    "conversion_rate": 0
})


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["unique_views", "unique_purchases", "conversion_rate"],
    outputCol="features"
)


# COMMAND ----------

train_df, test_df = spark_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
    featuresCol="features",
    labelCol="total_revenue"
)


# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[assembler, lr])
pipeline_model = pipeline.fit(train_df)


# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

predictions = pipeline_model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="total_revenue",
    predictionCol="prediction",
    metricName="r2"
)

r2 = evaluator.evaluate(predictions)
print(f"Spark Linear Regression R²: {r2:.4f}")
