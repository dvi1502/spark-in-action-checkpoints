from pyspark.sql import (SparkSession, functions as F)

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Column addition") \
        .master("local[*]")\
        .getOrCreate()

    df = spark.read.format("csv").option("header", True).load("./data/goodreads/books.csv")

    df = df.withColumn("main_author", F.split(F.col("authors"), "-").getItem(0))

    df.show(5);

    spark.stop();
