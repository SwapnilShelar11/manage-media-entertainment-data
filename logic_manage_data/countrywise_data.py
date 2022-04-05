from pyspark.sql.functions import col, split ,round

def country_data_manage(netflix_df,write_path):
    result_country_df=netflix_df.filter((col("country") != "No Data")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    result_country_movie_df=netflix_df.filter(col("type").like("Movie")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name"), col("type")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    result_country_tvshow_df=netflix_df.filter(col("type").like("TV Show")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name"), col("type")).count() \
        .withColumn("percentage", round((col("count") / netflix_df.count()) * 100, 2)) \
        .orderBy(col("count").desc()).limit(10)

    result_country_df.show()
    result_country_movie_df.show()
    result_country_tvshow_df.show()

    result_country_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_df")
    result_country_movie_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_movie_df")
    result_country_tvshow_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}countrywise_data//result_country_tvshow_df")





