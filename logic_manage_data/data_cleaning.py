from pyspark.sql.functions import col, coalesce, lit, to_date
def dataframe_cleaning(df):
    return df.select(col("show_id"), col("type"), col("title"),
              coalesce(col("director"), lit("No Data")).alias("director"),
              coalesce(col("cast"), lit("No Data")).alias("cast"),
              coalesce(col("country"), lit("No Data")).alias("country"),
              coalesce(to_date(col("date_added"), "MMMM dd, yyyy"), to_date(lit('0000-00-00'))).alias("date_added"),
              col("release_year").cast('int'),
              coalesce(col("rating"), lit("No Data")).alias("rating"),
              coalesce(col("duration"), lit("No Data")).alias("duration"),
              col("listed_in"), col("description"))