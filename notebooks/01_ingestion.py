"""
Ingestion Pipeline for Citi Bike Data

Steps:
1. Download zipped CSV from public S3
2. Extract file in memory
3. Convert to Spark DataFrame
4. Write to Bronze Delta table
"""

import requests
from xml.etree import ElementTree
import zipfile
import io
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

BASE_URL = "https://s3.amazonaws.com/tripdata"
BUCKET_LIST_URL = f"{BASE_URL}?list-type=2&prefix=2024"


def list_zip_files():
    """Fetch list of zip files from S3 bucket using XML API"""
    r = requests.get(BUCKET_LIST_URL)
    tree = ElementTree.fromstring(r.content)
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    
    return [
        el.text for el in tree.findall(f".//{ns}Key")
        if el.text.endswith(".zip")
    ]

def download_and_extract(url: str):
    """Download zip file and return Pandas DataFrame"""
    response = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    
    file_name = [f for f in z.namelist() if f.endswith(".csv")][0]
    pdf = pd.read_csv(z.open(file_name), low_memory=False, dtype=str)
    
    return pdf, file_name

def transform_to_spark(spark: SparkSession, pdf: pd.DataFrame, file_name: str, url: str):
    """Convert Pandas DF to Spark DF and add metadata"""
    return (
        spark.createDataFrame(pdf)
        .withColumn("_source_file", lit(file_name))
        .withColumn("_source_url", lit(url))
        .withColumn("_ingested_at", current_timestamp())
    )

def write_to_bronze(df):
    """Write DataFrame to Bronze Delta table"""
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("workspace.bronze.citibike_trips")

def run_pipeline():
    spark = SparkSession.builder.getOrCreate()
    
    files = list_zip_files()
    
    for zip_name in files:
        url = f"{BASE_URL}/{zip_name}"
        print(f"⬇️ Downloading: {zip_name}")
        
        pdf, file_name = download_and_extract(url)
        df = transform_to_spark(spark, pdf, file_name, url)
        
        write_to_bronze(df)
        
        print(f"✅ Done — {len(pdf):,} rows written")


if __name__ == "__main__":
    run_pipeline()
