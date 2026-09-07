# =========================================================
# GOLD LAYER - Fabric Notebook (PySpark)
# Reads Silver tables -> builds star schema + business tables
# =========================================================
from pyspark.sql import functions as F

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

dim_users = spark.read.table(f"{SILVER_SCHEMA}.dim_users")
dim_restaurant = spark.read.table(f"{SILVER_SCHEMA}.dim_restaurant")
bridge_cuisine = spark.read.table(f"{SILVER_SCHEMA}.bridge_restaurant_cuisine")
fact_orders = spark.read.table(f"{SILVER_SCHEMA}.fact_orders")

# Drop bad-quality rows for Gold (Silver kept them flagged for auditing;
# Gold is meant for direct reporting/Power BI consumption)
fact_orders_clean = fact_orders.filter(
    (~F.col("is_missing_restaurant")) &
    (~F.col("is_orphan_restaurant")) &
    (~F.col("is_negative_amount"))
).drop("is_missing_restaurant", "is_orphan_restaurant", "is_negative_amount")

# -------------------------------------------------
# 1. DIM_DATE (surrogate date key, ready for Power BI relationships)
# -------------------------------------------------
date_bounds = fact_orders_clean.select(
    F.min("order_date").alias("min_date"), F.max("order_date").alias("max_date")
).collect()[0]

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{date_bounds.min_date}'), "
              f"to_date('{date_bounds.max_date}'), interval 1 day)) as order_date")
    .withColumn("DateKey", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("order_date"))
    .withColumn("MonthNum", F.month("order_date"))
    .withColumn("MonthName", F.date_format("order_date", "MMMM"))
    .withColumn("Quarter", F.quarter("order_date"))
    .withColumn("DayOfWeek", F.date_format("order_date", "EEEE"))
)

(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_date"))

# -------------------------------------------------
# 2. DIM_USERS / DIM_RESTAURANT carried forward as-is
# -------------------------------------------------
(dim_users.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_users"))

(dim_restaurant.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_restaurant"))

(bridge_cuisine.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.bridge_restaurant_cuisine"))

# -------------------------------------------------
# 3. FACT_ORDERS (grain: one row per order, with DateKey for star schema)
# -------------------------------------------------
fact_orders_gold = fact_orders_clean.withColumn(
    "DateKey", F.date_format("order_date", "yyyyMMdd").cast("int")
)

(fact_orders_gold.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_orders"))

# -------------------------------------------------
# 4. BUSINESS-READY AGGREGATE TABLES
# -------------------------------------------------

# 4a. Monthly sales summary
monthly_sales = (
    fact_orders_gold
    .join(dim_date, "DateKey")
    .groupBy("Year", "MonthNum", "MonthName")
    .agg(
        F.sum("Sales_amount").alias("Total_Sales"),
        F.sum("Sales_QTY").alias("Total_Qty"),
        F.countDistinct("User_id").alias("Distinct_Users"),
        F.count("*").alias("Order_Count")
    )
    .orderBy("Year", "MonthNum")
)
(monthly_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.monthly_sales_summary"))

# 4b. Restaurant performance
restaurant_performance = (
    fact_orders_gold
    .groupBy("Restaurant_id")
    .agg(
        F.sum("Sales_amount").alias("Total_Sales"),
        F.sum("Sales_QTY").alias("Total_Qty"),
        F.count("*").alias("Order_Count")
    )
    .join(dim_restaurant, "Restaurant_id", "left")
    .select(
        "Restaurant_id", "Name", "City", "Rating", "Rating_count_bucket",
        "Total_Sales", "Total_Qty", "Order_Count"
    )
    .orderBy(F.desc("Total_Sales"))
)
(restaurant_performance.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.restaurant_performance"))

# 4c. Sales by user demographic segment
user_segment_sales = (
    fact_orders_gold
    .join(dim_users, "User_id")
    .groupBy("Gender", "Marital_Status", "Occupation")
    .agg(
        F.sum("Sales_amount").alias("Total_Sales"),
        F.count("*").alias("Order_Count"),
        F.countDistinct("User_id").alias("Distinct_Users")
    )
    .orderBy(F.desc("Total_Sales"))
)
(user_segment_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.user_segment_sales"))

# 4d. Sales by cuisine
cuisine_sales = (
    fact_orders_gold
    .join(bridge_cuisine, "Restaurant_id")
    .groupBy("Cuisine")
    .agg(
        F.sum("Sales_amount").alias("Total_Sales"),
        F.count("*").alias("Order_Count")
    )
    .orderBy(F.desc("Total_Sales"))
)
(cuisine_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.cuisine_sales"))

print("Gold layer complete: dim_date, dim_users, dim_restaurant, bridge_restaurant_cuisine, "
      "fact_orders, monthly_sales_summary, restaurant_performance, user_segment_sales, cuisine_sales")
