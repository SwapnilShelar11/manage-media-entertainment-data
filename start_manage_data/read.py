from start_session import spark

#read data from respective path
def read_data(fileFormat,readPath):
    return spark.read \
        .option("header", True) \
        .format(fileFormat) \
        .option("inferSchema", True) \
        .load(readPath)