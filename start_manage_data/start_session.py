from pyspark.sql import SparkSession

#start spark session
spark=SparkSession\
    .builder\
    .master("local")\
    .getOrCreate()