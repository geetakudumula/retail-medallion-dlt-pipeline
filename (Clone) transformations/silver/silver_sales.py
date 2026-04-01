from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

# Silver Layer - Cleaned and validated sales data
@dp.materialized_view(
    comment="Silver layer - cleaned sales data with computed columns"
)
def silver_sales():
    """
    Read from bronze pipeline table and add computed columns:
    - total_amount: price * quantity
    - order_date: date extracted from order_ts timestamp
    """
    df = spark.read.table("bronze_sales")
    
    return df.withColumn("total_amount", col("price") * col("quantity")) \
             .withColumn("order_date", to_date(col("order_ts")))
