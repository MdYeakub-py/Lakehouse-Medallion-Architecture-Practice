# =========================================================
# SILVER LAYER - Fabric Notebook (PySpark)
# Reads Bronze tables -> cleans -> writes Silver Delta tables
# =========================================================
# NOTE: Adjust lakehouse/schema names below to match your workspace.
# Example assumes Bronze tables are already registered as:
#   bronze.users, bronze.restaurant, bronze.orders
# If your Bronze tables live in a different Lakehouse, use the
# three-part name: LakehouseName.dbo.TableName or attach the
# Bronze Lakehouse to this notebook first.

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# -------------------------------------------------
# 1. DIM_USERS
# -------------------------------------------------
df_users = spark.read.table(f"{BRONZE_SCHEMA}.users")

dim_users = (
    df_users
    .dropDuplicates(["User_id"])
    .withColumn("Name", F.trim(F.col("Name")))
    .withColumn("Gender", F.trim(F.col("Gender")))
    .withColumn("Marital_Status", F.trim(F.col("Marital_Status")))
    .withColumn(
        "Occupation",
        F.when(F.trim(F.col("Occupation")) == "Self Employeed", "Self Employed")
         .otherwise(F.trim(F.col("Occupation")))
    )
    .withColumn("Age", F.col("Age").cast(IntegerType()))
)

(dim_users.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.dim_users"))

# -------------------------------------------------
# 2. DIM_RESTAURANT
# -------------------------------------------------
df_rest = spark.read.table(f"{BRONZE_SCHEMA}.restaurant")

dim_restaurant = (
    df_rest
    .dropDuplicates(["Restaurant_id"])
    .withColumn(
        "Rating",
        F.when(F.trim(F.col("Rating")) == "--", None)
         .otherwise(F.col("Rating")).cast(DoubleType())
    )
    .withColumn(
        "Rating_count_bucket", F.trim(F.col("Rating_count"))  # keep text bucket as-is
    )
    .withColumn(
        # numeric-ish rank for the bucket, useful for sorting/filtering in Gold
        "Rating_count_rank",
        F.when(F.col("Rating_count") == "Too Few Ratings", 0)
         .when(F.col("Rating_count") == "20+ ratings", 1)
         .when(F.col("Rating_count") == "50+ ratings", 2)
         .when(F.col("Rating_count") == "100+ ratings", 3)
         .when(F.col("Rating_count") == "500+ ratings", 4)
         .when(F.col("Rating_count") == "1K+ ratings", 5)
         .when(F.col("Rating_count") == "5K+ ratings", 6)
         .when(F.col("Rating_count") == "10K+ ratings", 7)
         .otherwise(None)
    )
    .drop("Rating_count")
    .withColumn("City", F.trim(F.col("City")))
    .drop("Country")  # single-value column, no analytical value
)

(dim_restaurant.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.dim_restaurant"))

# -------------------------------------------------
# 2b. BRIDGE_RESTAURANT_CUISINE (normalize comma-separated Cuisine)
# -------------------------------------------------
bridge_cuisine = (
    df_rest.select("Restaurant_id", "Cuisine")
    .withColumn("Cuisine", F.explode(F.split(F.col("Cuisine"), ",")))
    .withColumn("Cuisine", F.trim(F.col("Cuisine")))
    .dropDuplicates(["Restaurant_id", "Cuisine"])
)

(bridge_cuisine.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.bridge_restaurant_cuisine"))

# -------------------------------------------------
# 3. FACT_ORDERS
# -------------------------------------------------
df_orders = spark.read.table(f"{BRONZE_SCHEMA}.orders")

valid_restaurant_ids = dim_restaurant.select("Restaurant_id").withColumnRenamed("Restaurant_id", "_valid_id")

fact_orders_base = (
    df_orders
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("Restaurant_id", F.col("Restaurant_id").cast(IntegerType()))
    .withColumn("Sales_QTY", F.col("Sales_QTY").cast(IntegerType()))
    .withColumn("Sales_amount", F.col("Sales_amount").cast(DoubleType()))
    .drop("currency")  # single-value column (INR only)
    .withColumn("is_missing_restaurant", F.col("Restaurant_id").isNull())
    .withColumn("is_negative_amount", F.col("Sales_amount") < 0)
)

# left-anti style flag via broadcast join, instead of collect()-ing IDs to the driver
fact_orders = (
    fact_orders_base
    .join(F.broadcast(valid_restaurant_ids), fact_orders_base.Restaurant_id == F.col("_valid_id"), "left")
    .withColumn(
        "is_orphan_restaurant",
        F.col("Restaurant_id").isNotNull() & F.col("_valid_id").isNull()
    )
    .drop("_valid_id")
)

(fact_orders.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.fact_orders"))

print("Silver layer complete: dim_users, dim_restaurant, bridge_restaurant_cuisine, fact_orders")
