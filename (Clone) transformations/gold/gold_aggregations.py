from pyspark import pipelines as dp
from pyspark.sql.functions import sum as spark_sum, count, desc

# Gold Layer - Business metrics and aggregations

@dp.materialized_view(
    comment="Gold layer - total sales by product and category"
)
def gold_sales_by_product():
    """Aggregate sales by product and category"""
    df = spark.read.table("silver_sales")
    
    return df.groupBy("product", "category") \
        .agg(
            spark_sum("total_amount").alias("total_sales"),
            count("order_id").alias("order_count")
        ) \
        .orderBy(desc("total_sales"))


@dp.materialized_view(
    comment="Gold layer - daily sales trends"
)
def gold_sales_by_day():
    """Aggregate sales by order date"""
    df = spark.read.table("silver_sales")
    
    return df.groupBy("order_date") \
        .agg(
            spark_sum("total_amount").alias("total_sales"),
            count("order_id").alias("order_count")
        ) \
        .orderBy("order_date")


@dp.materialized_view(
    comment="Gold layer - top customers by spending"
)
def gold_top_customers():
    """Aggregate customer spending"""
    df = spark.read.table("silver_sales")
    
    return df.groupBy("customer_id", "city") \
        .agg(
            spark_sum("total_amount").alias("total_spent"),
            count("order_id").alias("order_count")
        ) \
        .orderBy(desc("total_spent"))
