# Databricks notebook source
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split

# Load data from your existing gold table
df = spark.table("workspace.default.gold_product").toPandas()
df.head()

# COMMAND ----------


# Features & target
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
X = df[["unique_views", "unique_purchases", "conversion_rate"]]
y = df["total_revenue"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2
)

#scalar data
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

def model_run(model):
    with mlflow.start_run(run_name="product_revenue_regression_v1"):

    # Log metadata
        mlflow.log_param("model_type", "Regression")
        mlflow.log_param(
        "features",
        "unique_views, unique_purchases, conversion_rate"
    )
        mlflow.log_param("target", "total_revenue")

    # Train model
        model.fit(X_train, y_train)

    # Evaluate
        score = model.score(X_test, y_test)
        mlflow.log_metric("r2_score", score)

    # Log model
        mlflow.sklearn.log_model(model, "model")

    print(f"R² Score: {score:.4f}")

# COMMAND ----------

from sklearn.linear_model import LinearRegression , Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

model1 = LinearRegression()
model2 = Ridge()
model3 = RandomForestRegressor()
model4 = GradientBoostingRegressor()
model_run(model1)
model_run(model2)
model_run(model3)
model_run(model4)

