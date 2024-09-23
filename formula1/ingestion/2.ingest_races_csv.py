import sys
sys.path.append("/Workspace/Formula1/dependencies/pipeline")

# COMMAND ----------

# imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pipeline import ingest_data

# COMMAND ----------

load_options_dict = {
    "header": "True",
}
schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)
column_rename_dict = {
    "raceId": "race_id",
    "year": "race_year",
    "circuitId": "circuit_id",
}
columns_to_drop = ["date", "time", "url"]

# COMMAND ----------

df = ingest_data(
    spark=spark,
    source_file_path="/mnt/f1databrickscourself/raw/races.csv",
    target_path="/mnt/f1databrickscourself/processed/races",
    format="csv",
    concat_date_and_time=True,
    schema=schema,
    load_options_dict=load_options_dict, 
    column_rename_dict=column_rename_dict, 
    columns_to_drop=columns_to_drop
)
