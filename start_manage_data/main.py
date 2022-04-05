import configparser

from pyspark.sql.functions import col, coalesce, lit, to_date, date_format, split, round
from logic_manage_data.data_cleaning import dataframe_cleaning
from logic_manage_data.countrywise_data import country_data_manage
from read import read_data

config = configparser.ConfigParser()
config.read("..\\config.properties")
read_path=config.get('ReadSection','readPath')
write_path=config.get('WriteSection','writePath')
fileFormat=config.get('ReadSection','fileFormat')


netflix_df=dataframe_cleaning(read_data(fileFormat,read_path))
#country_data_manage(netflix_df,write_path)
#netflix_df.show(truncate=False)
netflix_df.select("rating").distinct().show(50)
#netflix_df.groupBy(col("rating")).count().show(truncate=False)


