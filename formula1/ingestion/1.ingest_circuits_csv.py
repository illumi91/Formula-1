import sys
sys.path.append("/Workspace/Formula1/dependencies/pipeline")

# COMMAND ----------

# imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pipeline import ingest_data

load_options_dict = {
    "header": "True",
}
schema = StructType(
    [
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)
column_rename_dict = {
    "circuitId": "circuit_id",
    "circuitRef": "circuit_ref",
    "lat": "latitude",
    "long": "longitude",
    "alt": "altitude",
}
columns_to_drop = ["url"]

# COMMAND ----------

df = ingest_data(
    spark=spark,
    source_file_path="/mnt/f1databrickscourself/raw/circuits.csv",
    target_path="/mnt/f1databrickscourself/processed/circuits",
    format="csv",
    schema=schema,
    load_options_dict=load_options_dict, 
    column_rename_dict=column_rename_dict, 
    columns_to_drop=columns_to_drop
)
