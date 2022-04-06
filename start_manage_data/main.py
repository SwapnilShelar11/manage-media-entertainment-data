import configparser
import time

from pyspark.sql.functions import col, coalesce, lit, to_date, date_format, split, round, count, year, substring, \
    length, avg
from pyspark.sql.window import Window

from logic_manage_data.data_cleaning import dataframe_cleaning
#from logic_manage_data.countrywise_data import country_data_manage
from logic_manage_data.duration_data import manage_duration_data
#from logic_manage_data.rating_data import rating_data_manage
from read import read_data

config = configparser.ConfigParser()
config.read("..\\config.properties")
read_path=config.get('ReadSection','readPath')
write_path=config.get('WriteSection','writePath')
fileFormat=config.get('ReadSection','fileFormat')


netflix_df=dataframe_cleaning(read_data(fileFormat,read_path))
#country_data_manage(netflix_df,write_path)
#rating_data_manage(netflix_df,write_path)
#manage_duration_data(netflix_df,write_path)
#netflix_df.select(year(col("date_added"))).show()
netflix_df.filter(col("date_added").like("2021%")).show()
#netflix_df.groupBy("release_year").count().orderBy(col("count").desc()).show(50)
#netflix_df.filter(col("date_added").like("2021%")).filter(col("country")=="India").show()
#netflix_df.filter((col("date_added").like("2021%")) & (col("country")=="India")).show()



netflix_df.groupBy(col("release_year")).count()\
    .orderBy(col("count").desc()).show()