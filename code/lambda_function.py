########################################################################################
# Script name: lambda_function.py
# Description: This script fetches historical records of Australian natural disaster,
# global temperature index and Australian maximum temperature. And it loads the raw
# datasets into S3 bucket ingress.
# Creator: Travis Hong
# Repository: https://github.com/TravisH0301/global_warming_disaster
########################################################################################

# Import libraries
import pandas as pd
import re
import datetime
import pytz
import requests
import boto3
from io import StringIO


def fetch_nat_dis():
    """For natural disaster data, the Australian disaster dataset is used.
    Source: https://data.gov.au/dataset/ds-dga-26e2ebff-6cd5-4631-9653-18b56526e354
    /details?q=disaster
    """
    # Fetch Australian natural disaster dataset from API
    api_url = url_nat_dis
    response = requests.get(api_url)
    data = response.json()
    # Flatten json data and convert into dataframe
    df_dis_raw = pd.json_normalize(data["result"]["records"])

    return df_dis_raw

def fetch_glo_temp():
    """For this project, global land-ocean temperature index is selected
    as a global warming indicator.
    Source: https://climate.nasa.gov/vital-signs/global-temperature/
    """
    # Fetch global temperature data from API
    api_url = url_glo_temp
    response = requests.get(api_url)
    data = response.text
    # Extract headers from text data
    headers = [
        header for header in data.split("\n")[3].strip().split(" ") if header != ""
    ]
    # Extract values from text data
    year_li = []
    temp_li = []
    temp_lowess_li = []
    for line in data.split("\n")[5:-1]:
        year, temp, temp_lowess = [value for value in line.split(" ") if value != ""]
        year_li.append(year)
        temp_li.append(temp)
        temp_lowess_li.append(temp_lowess)
    # Create dataframe from extracted data
    df_glo_raw = pd.DataFrame(dict(zip(headers, [year_li, temp_li, temp_lowess_li])))

    return df_glo_raw

def fetch_temp_ano():
    """For Temperature anomalies, Australia's maximum temperature anomalies 
    relative to 1961-1990 average are used.
    Source: http://www.bom.gov.au/state-of-the-climate/
    Monthly temperature anomalies: http://www.bom.gov.au/web01/ncc/www/cli_chg
    /timeseries/tmax/allmonths/aus/latest.txt
    """
    # Fetch annual max. temperature anomalies from API
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }
    api_url_mon = url_temp_ano
    response_mon = requests.get(api_url_mon, headers=headers)
    data_mon = response_mon.text
    # Extract data from text file
    dates_mon = []
    vals_mon = []
    for line in data_mon.split("\n")[:-1]:
        date, val = [value for value in line.split(" ") if value != ""]
        date_new = date[:4] + date[-2:]
        dates_mon.append(date_new)
        vals_mon.append(val)
    # Create a dataframe from extracted data
    columns_mon = ["Date", "Anomaly"]
    df_ano_raw = pd.DataFrame(data=zip(dates_mon, vals_mon), columns=columns_mon)

    return df_ano_raw

def filter_nat_dis(df_dis_raw):
    # Remove duplicates
    df_dis_fil1 = df_dis_raw.drop_duplicates(
        subset=["title", "startDate", "endDate", "lat", "lon"]
    )
    # Drop null rows
    df_dis_fil2 = df_dis_fil1.dropna(how="all")
    
    return df_dis_fil2

def filter_glo_temp(df_glo_raw):
    # Remove duplicates
    df_glo_fil1 = df_glo_raw.drop_duplicates(subset=["Year", "Lowess(5)"])
    # Drop null rows
    df_glo_fil2 = df_glo_fil1.dropna(how="all")

    return df_glo_fil2

def filter_temp_ano(df_ano_raw):
    # Remove duplicates
    df_ano_fil1 = df_ano_raw.drop_duplicates()
    # Drop null rows
    df_ano_fil2 = df_ano_fil1.dropna(how="all")

    return df_ano_fil2

def save_df_to_s3(df, bucket_name, file_name):
    # Create a CSV buffer
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Create an S3 client
    s3 = boto3.client("s3")

    # Put the CSV buffer into the S3 object
    s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=bucket_name,
        Key=file_name
    )


# Define main function
def main():
    # Define variables
    ## API URLs
    url_nat_dis = "https://data.gov.au/data/api/3/action/datastore_search?resource_id=ad5c6594-571e-4874-994c-a9f964d789df&limit=50000"
    url_glo_temp = "https://data.giss.nasa.gov/gistemp/graphs/graph_data/Global_Mean_Estimates_based_on_Land_and_Ocean_Data/graph.txt"
    url_temp_ano = "http://www.bom.gov.au/web01/ncc/www/cli_chg/timeseries/tmax/allmonths/aus/latest.txt"
    ## S3 bucket
    target_bucket = "gwd-ingress"
    file_nat_dis = "df_dis_ingress.csv"
    file_glo_temp = "df_glo_ingress.csv"
    file_temp_ano = "df_ano_ingress.csv"
    ## Date
    timezone = pytz.timezone('Australia/Melbourne')
    today = datetime.datetime.now(timezone).strftime("%Y-%m-%d")

    # Fetch datasets
    print("Fetching datasets...")
    ## Natural disaster
    df_dis_raw = fetch_nat_dis()
    ## Global temperature
    df_glo_raw = fetch_glo_temp()
    ## Temperature anomalies
    df_ano_raw = fetch_temp_ano()

    # Initial transformation
    print("Dropping invalid records from datasets...")
    ## Natural disaster
    df_dis_filter = filter_nat_dis(df_dis_raw)
    ## Global temperature
    df_glo_filter = filter_glo_temp(df_glo_raw)
    ## Temperature anomalies
    df_ano_filter = filter_temp_ano(df_ano_raw)

    # Load datasets into S3 bucket ingress
    print("Loading datasets into S3 bucket ingress...")
    save_df_to_s3(df_dis_filter, target_bucket, file_nat_dis)
    save_df_to_s3(df_glo_filter, target_bucket, file_glo_temp)
    save_df_to_s3(df_ano_filter, target_bucket, file_temp_ano)

    print("Data load has completed.")

def lambda_handler(event, context):
    # Run process
    main()