"""
Start spark session
"""
import logging
from pyspark.sql import SparkSession

logging.info("spark session starting...")
spark = SparkSession \
    .builder \
    .master("local") \
    .getOrCreate()
