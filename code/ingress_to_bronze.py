########################################################################################
# Script name: ingress_to_bronze.py
# Description: This script extracts raw datasets from ingress bucket and stores them
# in bronze bucket in delta lake format.
# Creator: Travis Hong
# Repository: https://github.com/TravisH0301/global_warming_disaster
########################################################################################

# Import libraries
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from delta import *


def main():
    # Load the raw data from S3 ingress bucket
    logger.info("Loading raw data from S3 ingress bucket...")
    df_dis_ingress = spark.read.csv(f"{s3_input_path}df_dis_ingress.csv", header=True)
    df_glo_ingress = spark.read.csv(f"{s3_input_path}df_glo_ingress.csv", header=True)
    df_ano_ingress = spark.read.csv(f"{s3_input_path}df_ano_ingress.csv", header=True)

    # Save bookmark in case of reprocessing
    job.commit()

    # Replace spaces in column names with underscores
    """Delta lake doesn't allow certain characters (" ,;{}()\n\t=") in column names."""
    ## Replace spaces with underscores
    df_dis_ingress = df_dis_ingress.toDF(*(c.replace(' ', '_') for c in df_dis_ingress.columns))
    df_glo_ingress = df_glo_ingress.toDF(*(c.replace(' ', '_') for c in df_glo_ingress.columns))
    df_ano_ingress = df_ano_ingress.toDF(*(c.replace(' ', '_') for c in df_ano_ingress.columns))
    ## Remove parenthesises
    df_dis_ingress = df_dis_ingress.toDF(*(c.replace('(', '') for c in df_dis_ingress.columns))
    df_dis_ingress = df_dis_ingress.toDF(*(c.replace(')', '') for c in df_dis_ingress.columns))
    df_glo_ingress = df_glo_ingress.toDF(*(c.replace('(', '') for c in df_glo_ingress.columns))
    df_glo_ingress = df_glo_ingress.toDF(*(c.replace(')', '') for c in df_glo_ingress.columns))
    df_ano_ingress = df_ano_ingress.toDF(*(c.replace('(', '') for c in df_ano_ingress.columns))
    df_ano_ingress = df_ano_ingress.toDF(*(c.replace(')', '') for c in df_ano_ingress.columns))

    # Write the data to S3 bronze bucket in Delta Lake format
    logger.info("Writing data in delta lake to S3 bronze bucket...")
    df_dis_ingress.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_dis_bronze")
    df_glo_ingress.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_glo_bronze")
    df_ano_ingress.write.format("delta").mode("overwrite").save(f"{s3_output_path}df_ano_bronze")

    logger.info("Data load is completed.")


if __name__ == "__main__":
    # Create Spark session with Glue context
    ## Initialize the Spark session with Delta Lake configurations
    spark = SparkSession.builder \
        .appName("IngressToBronze") \
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

    # Define S3 path
    s3_input_path = "s3://gwd-ingress/"
    s3_output_path = "s3://gwd-bronze/"

    # Run main process
    main()