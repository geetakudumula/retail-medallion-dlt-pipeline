from pyspark import pipelines as dp

# Bronze Layer - Raw sales data ingestion
@dp.materialized_view(
    comment="Bronze layer - raw sales data from existing Delta table"
)
def bronze_sales():
    """Read existing bronze Delta table"""
    return spark.read.format("delta").table("demo_retail.bronze_sales")
