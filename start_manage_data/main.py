"""
From here run code
"""
import configparser

from logic_manage_data.countrywise_data import country_data_manage
from logic_manage_data.data_cleaning import dataframe_cleaning
from logic_manage_data.duration_data import manage_duration_data
from logic_manage_data.rating_data import rating_data_manage
from read import read_data

config = configparser.ConfigParser()
config.read("..\\config.properties")
read_path=config.get('ReadSection','readPath')
write_path=config.get('WriteSection','writePath')
fileFormat=config.get('ReadSection','fileFormat')

netflix_df=dataframe_cleaning(read_data(fileFormat,read_path))
country_data_manage(netflix_df,write_path)
rating_data_manage(netflix_df,write_path)
manage_duration_data(netflix_df,write_path)
