########################################################################################
# Script name: silver_to_gold.py
# Description: This script extracts transformed datasets from silver bucket and creates
# consolidated, aggregated and denormalised datasets for gold bucket.
# Creator: Travis Hong
# Repository: https://github.com/TravisH0301/global_warming_disaster
########################################################################################

# Import libraries
import sys
import statsmodels.api as sm
import boto3
from botocore.exceptions import ClientError

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from delta import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


def denorm_ano(df_ano):
    # Denormalise by adding year and month columns
    df_ano_denorm = df_ano.withColumn("Year", F.year(df_ano["Date"]))
    df_ano_denorm = df_ano_denorm.withColumn("Month", F.month(df_ano["Date"]))

    return df_ano_denorm

def apply_smoothing(df, frac=0.2):
    # Prepare data for Lowess smoothing
    df_pd = df.toPandas()

    # Apply Lowess smoothing
    x = df_pd["Year"].values
    y = df_pd["Disaster_Count"].values
    xy_lowess = sm.nonparametric.lowess(exog=x, endog=y, is_sorted=True, frac=frac)
    y_lowess = xy_lowess[:, 1]

    # Add Lowess smoothed data to the DataFrame
    df_pd["Disaster_Count_Lowess"] = y_lowess
    df_lowess = spark.createDataFrame(df_pd)

    return df_lowess

def consolidate(df_dis, df_glo, df_ano_denorm):
    # Aggregate disaster dataset into yearly count
    df_dis = df_dis.withColumn("Year", F.year(df_dis["Date"]))
    df_dis_agg_tot = df_dis.groupBy("Year").agg(F.count("Date").alias("Disaster_Count"))

    # Apply Lowess(5) smoothing on disaster count
    df_dis_agg_tot_smoothed = apply_smoothing(df_dis_agg_tot)

    # Aggregate temp anomaly (Lowess) dataset into yearly average
    df_ano_denorm_agg_avg = df_ano_denorm.groupBy("Year").agg(F.mean("Anomaly_Lowess").alias("Temp_Anomaly_Lowess"))

    # Merge aggregated datasets with global temp dataset
    df_con_year = (df_glo.join(df_dis_agg_tot_smoothed.drop("Disaster_Count"), on="Year", how="inner")
                         .join(df_ano_denorm_agg_avg, on="Year", how="inner"))

    return df_con_year

def agg_dis(df_dis):
    # Aggregate disaster dataset into categorical count 
    df_dis_agg = df_dis.groupBy("Category").agg(F.count("Date").alias("Count"))
    
    # Drop categories with less than 5% portion of disaster counts
    total_count = df_dis_agg.agg(F.sum("Count")).collect()[0][0]
    df_dis_agg = df_dis_agg.withColumn("Portion", (F.col("Count") / total_count) * 100)
    df_dis_agg = df_dis_agg.filter(df_dis_agg["Portion"] > 5).drop("Portion")

    return df_dis_agg


def main():
    # Load the raw data from S3 silver bucket
    logger.info("Loading data from S3 silver bucket...")
    df_dis_silver = spark.read.format("delta").load(f"{s3_input_path}df_dis_silver")
    df_glo_silver = spark.read.format("delta").load(f"{s3_input_path}df_glo_silver")
    df_ano_silver = spark.read.format("delta").load(f"{s3_input_path}df_ano_silver")

    # Save bookmark in case of reprocessing
    job.commit()

    # Create analytical datasets
    logger.info("Creating analytical datasets...")
    # Create a denormalised temperature anomalies dataset
    df_ano_denorm = denorm_ano(df_ano_silver)
    # Consolidate all datasets on yearly aggregation with Lowess smoothing
    df_con_year = consolidate(df_dis_silver, df_glo_silver, df_ano_denorm)
    # Aggregate natural disaster dataset on disaster categories
    df_dis_agg = agg_dis(df_dis_silver)

    # Write analytical datasets to S3 gold bucket in Delta Lake format
    logger.info("Writing analytical datasets in delta lake to S3 gold bucket...")
    df_con_year.write.format("delta").mode("overwrite").save(f"{s3_output_path}consolidated_measures")
    df_dis_agg.write.format("delta").mode("overwrite").save(f"{s3_output_path}categorical_disasters")
    df_ano_denorm.write.format("delta").mode("overwrite").save(f"{s3_output_path}temp_anomalies")

    # Generate Manifest files for Athena/Catalog
    logger.info("Generating manifest files...")
    deltaTable_dis = DeltaTable.forPath(spark, f"{s3_output_path}consolidated_measures")
    deltaTable_dis.generate("symlink_format_manifest")
    deltaTable_glo = DeltaTable.forPath(spark, f"{s3_output_path}categorical_disasters")
    deltaTable_glo.generate("symlink_format_manifest")
    deltaTable_ano = DeltaTable.forPath(spark, f"{s3_output_path}temp_anomalies")
    deltaTable_ano.generate("symlink_format_manifest")

    logger.info("Data load is completed.")


if __name__ == "__main__":
    # Create Spark session with Glue context
    ## Initialize the Spark session with Delta Lake configurations
    spark = SparkSession.builder \
        .appName("SilverToGold") \
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

    # Create boto client for Glue data catalog
    glue = boto3.client('glue')

    # Define variables
    ## S3 path
    s3_input_path = "s3://gwd-silver/"
    s3_output_path = "s3://gwd-gold/"

    # Run main process
    main()