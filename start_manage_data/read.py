"""
Read data from respective path
"""
from start_session import spark
def read_data(file_format,read_path):
    """
    Read data
    :param file_format:
    :param read_path:
    :return: Read data from respective path and file format
    """
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    return spark.read \
        .option("header", True) \
        .format(file_format) \
        .option("inferSchema", True) \
        .load(read_path)
