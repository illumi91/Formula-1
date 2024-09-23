from datetime import datetime
from pyspark.sql.functions import concat, current_timestamp, lit, to_timestamp

def create_df(spark, source_file_path, schema, format, **kwargs):
    """Loads a DataFrame from a given source file path
    """
    reader = spark.read.format(format)
    if kwargs:
        for k, v in kwargs.items():
            reader = reader.option(k, v)
    return reader.load(source_file_path, schema=schema)

def rename_cols_df(df, **kwargs):
    """Renames columns in a DataFrame
    """
    if kwargs:
        for k, v in kwargs.items():
            df = df.withColumnRenamed(k, v)
    return df

def drop_cols(df, columns_to_drop):
    """Drops columns in a DataFrame
    """
    return df.drop(*columns_to_drop)

def add_ingestion_date_col(df):
    """Adds a column with the current timestamp to a DataFrame
    """
    return df.withColumn("ingestion_date", current_timestamp())

def concat_date_and_time_cols(df, column_name):
    """Concatenates two columns date and time in a DataFrame
    """
    df = df.withColumn(column_name, to_timestamp(concat(df.date, lit(" "), df.time), "yyyy-MM-dd HH:mm:ss"))
    return df

def write_df_to_parquet(df, target_path):
    """Writes a DataFrame to a Parquet file
    """
    df.write.mode("overwrite").format("parquet").save(target_path)

def ingest_data(
    spark,
    source_file_path, 
    target_path, format, 
    schema,
    concat_date_and_time=False,
    load_options_dict=None, 
    column_rename_dict=None, 
    columns_to_drop=None
):
    """Loads a DataFrame from a given source file path, performs 
    transformations and writes the result to a Parquet file
    """
    df = create_df(spark, source_file_path, schema, format, **load_options_dict)
    if concat_date_and_time:
        df = concat_date_and_time_cols(df, "race_timestamp")
    df = rename_cols_df(df, **column_rename_dict)
    df = drop_cols(df, columns_to_drop)
    df = add_ingestion_date_col(df)
    write_df_to_parquet(df, target_path)
    return df
