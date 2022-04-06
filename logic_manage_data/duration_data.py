from pyspark.sql.functions import col, avg, split, round




def manage_duration_data(netflix_df,write_path):
    duration_movie_data=netflix_df.filter(col("type") == "Movie").withColumn("dura", split(col("duration"), " ")[0].cast("int")) \
        .groupBy(split(col("country"), ",")[0].alias("country_name")).agg(round(avg(col("dura")), 2).alias("count")) \
        .orderBy(col("count").desc())
    duration_tv_df=netflix_df.filter(col("type") == "TV Show").groupBy(col("duration")).count() \
        .orderBy(col("count").desc())

    duration_movie_data.show()
    duration_tv_df.show()

    duration_movie_data.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}duration_data//duration_movie_df")
    duration_tv_df.repartition(1).write.option("header", "true").mode("overwrite") \
        .csv(f"{write_path}duration_data//duration_tv_df")