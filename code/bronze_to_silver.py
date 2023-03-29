########################################################################################
# Script name: bronze_to_silver.py
# Description: This script extracts and transforms datasets from S3 bronze bucket, and
# loads the transformed datasets into S3 silver bucket.
# Creator: Travis Hong
# Repository: https://github.com/TravisH0301/global_warming_disaster
########################################################################################

# Import libraries
import sys
import re
import statsmodels.api as sm

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from delta import *
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, BooleanType
from pyspark.sql.window import Window


# Define transformation functions
## Natural disaster data
def filter_columns(df):
    return df.select("title", "startDate", "endDate", "lat", "lon")

def extract_cat(title):
    try:
        category = re.findall("(.*?)\s-", title)[0]
    except:
        category = re.findall("(.*?)\s", title)[0]
        
    return category

def create_category_column(df):
    extract_cat_udf = F.udf(extract_cat, StringType())

    return df.withColumn("Category", extract_cat_udf(df["Title"]))

def filter_title(df, nat_dis_categories):
    df = create_category_column(df)
    df_filter = df.filter(df["Category"].isin(nat_dis_categories))

    return df_filter

def filter_coord(df, lat_aus_min, lat_aus_max, lon_aus_min, lon_aus_max):
    return df.filter((lat_aus_min <= df["lat"]) & (df["lat"] <= lat_aus_max) & (lon_aus_min <= df["lon"]) & (df["lon"] <= lon_aus_max))

def rename_columns(df):
    return df.withColumnRenamed("title", "Title") \
             .withColumnRenamed("startDate", "Start_Date") \
             .withColumnRenamed("endDate", "End_Date") \
             .withColumnRenamed("lat", "Lat") \
             .withColumnRenamed("lon", "Lon")

def create_date_column(df):
    return df.withColumn("Date_Str", F.regexp_extract("Start_Date", r"^(.*?)\s", 1)) \
             .withColumn("Date", F.coalesce(F.to_date("Date_Str", "M/d/yyyy"), F.to_date("Date_Str", "d/M/yyyy")))

def replace_env_cat(category, title):
    title = title.lower()

    if (category == "Environmental") or (category == "Envionmental"):
        if "heatwave" in title:
            return "Heatwave"
        elif "drought" in title:
            return "Drought"
        elif "bushfire" in title:
            return "Bushfire"
        else:
            return "Others"
    else:
        return category

def refine_environmental_categories(df):
    replace_env_cat_udf = F.udf(replace_env_cat, StringType())
    return df.withColumn("Category", replace_env_cat_udf(df["Category"], df["Title"]))

def drop_title_and_reorder_columns(df):
    return df.select("Category", "Date", "Lat", "Lon")

def transform_dis(df):
    df_dis_fil = filter_columns(df)
    df_dis_fil = filter_title(df_dis_fil, nat_dis_categories)
    df_dis_fil = filter_coord(df_dis_fil, lat_aus_min, lat_aus_max, lon_aus_min, lon_aus_max)
    df_dis_col = rename_columns(df_dis_fil)
    df_dis_col = create_date_column(df_dis_col)
    df_dis_col = refine_environmental_categories(df_dis_col)
    df_dis_silver = drop_title_and_reorder_columns(df_dis_col)

    return df_dis_silver

## Global temperature data
def transform_glo(df):
    # Filter columns
    df_glo_fil = df.select("Year", "Lowess5")
    # Rename columns
    df_glo_col = df_glo_fil.withColumnRenamed("Lowess5", "Global_Temp_Index_Lowess")
    # Change data types
    df_glo_silver = df_glo_col.withColumn("Year", F.col("Year").cast(IntegerType())) \
                           .withColumn("Global_Temp_Index_Lowess", F.col("Global_Temp_Index_Lowess").cast(FloatType()))

    return df_glo_silver

## Temperature anomalies data
def create_columns(df):
    # Convert date column from YYYYMM to YYYYMMDD by making it the first day of the month
    df_date = df.withColumn("Date", F.to_date(F.concat(df["Date"], F.lit("01")), "yyyyMMdd"))

    # Convert anomaly from string to float
    df_anomaly = df_date.withColumn("Anomaly", F.col("Anomaly").cast(FloatType()))

    return df_anomaly

def apply_smoothing(df_anomaly):
    # Prepare data for Lowess smoothing
    df_anomaly = df_anomaly.withColumn("row_num", F.row_number().over(Window.orderBy("Date")))
    df_anomaly_pd = df_anomaly.select("row_num", "Anomaly").toPandas()

    # Apply Lowess smoothing on Monthly Anomaly
    x = df_anomaly_pd["row_num"].values
    y = df_anomaly_pd["Anomaly"].values
    xy_lowess = sm.nonparametric.lowess(exog=x, endog=y, is_sorted=True, frac=0.2)
    y_lowess = xy_lowess[:, 1]

    # Add Lowess smoothed data to the DataFrame
    df_lowess_pd = df_anomaly_pd.copy()
    df_lowess_pd["Anomaly_Lowess"] = y_lowess
    df_lowess = spark.createDataFrame(df_lowess_pd)

    # Join the smoothed data back to the original DataFrame
    df_result = df_anomaly.join(df_lowess.select("row_num", "Anomaly_Lowess"), on="row_num", how="inner") \
                           .drop("row_num")

    return df_result

def transform_ano(df):
    df_ano_col = create_columns(df)
    df_ano_silver = apply_smoothing(df_ano_col)

    return df_ano_silver


def main():
    # Load the raw data from S3 bronze bucket
    logger.info("Loading raw data from S3 bronze bucket...")
    df_dis_bronze = spark.read.format("delta").load(f"{s3_input_path}df_dis_bronze")
    df_glo_bronze = spark.read.format("delta").load(f"{s3_input_path}df_glo_bronze")
    df_ano_bronze = spark.read.format("delta").load(f"{s3_input_path}df_ano_bronze")

    # Save bookmark in case of reprocessing
    job.commit()

    # Transform data
    logger.info("Processing raw datasets...")
    ## Natural disaster data
    df_dis_silver = transform_dis(df_dis_bronze)
    df_glo_silver = transform_glo(df_glo_bronze)
    df_ano_silver = transform_ano(df_ano_bronze)

    # Write transformed data to S3 silver bucket in Delta Lake format
    logger.info("Writing data in delta lake to S3 silver bucket...")
    df_dis_silver.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_dis_silver")
    df_glo_silver.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_glo_silver")
    df_ano_silver.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_ano_silver")

    logger.info("Data load is completed.")


if __name__ == "__main__":
    # Create Spark session with Glue context
    ## Initialize the Spark session with Delta Lake configurations
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    ## Initialize Glue context using the configured Spark session
    glueContext = GlueContext(spark)
    
    # Set custom logger
    logger = glueContext.get_logger()

    # Create Glue job
    logger.info("Creating Glue job...")
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Define variables
    ## S3 path
    s3_input_path = "s3://gwd-bronze/"
    s3_output_path = "s3://gwd-silver/"
    ## Natural disaster data transformation
    nat_dis_categories = [
        'Severe Storm',
        'Tsunami',
        'Landslide',
        'Envionmental',
        'Environmental',
        'Earthquake',
        'Cyclone',
        'Bushfire',
        'Tornado',
        'Hail',
        'Flood'
    ]
    lat_aus_min = -43.38
    lat_aus_max = -10.41
    lon_aus_min = 113.09
    lon_aus_max = 153.38

    # Run main process
    main()