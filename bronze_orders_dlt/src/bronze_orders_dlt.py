import dlt
from pyspark.sql.functions import current_timestamp, col

SOURCE_TABLE = "source.default.orders"

BRONZE_CATALOG = "bronze"
BRONZE_SCHEMA  = "default" #Any have anything based on business use case it is an testing schema.
BRONZE_TABLE   = "orders"

@dlt.table( #Delta Live Tables decorator: Creates and manages a table in Catalog
    name=f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}",
    comment="Bronze ingestion from source.default.orders (raw copy) with bronze_ingestion_date."
)
def bronze_orders():
    return (
        spark.table(SOURCE_TABLE) #Reads Source table
        .select(
            col("id").cast("int").alias("id"),
            col("category").cast("string").alias("category"),
            col("amount").cast("int").alias("amount"),
            col("ingestion_date").cast("timestamp").alias("ingestion_date")  # from source
        )
        .withColumn("bronze_ingestion_date", current_timestamp())            # added by bronze
    )

#df.write.format("delta").mode("append").saveAsTable("bronze.default.orders") {Works in Job} - NOT REQUIRED IN DLT, as dlt decorater handles it.