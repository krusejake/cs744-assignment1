from pyspark.sql import SparkSession
from pyspark.sql import functions as f
# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument("input")
# parser.add_argument("output")
# args = parser.parse_args()

spark = SparkSession \
    .builder \
    .appName("part2") \
    .config("spark.driver.memory", "30g") \
    .config("spark.executor.memory", "30g") \
    .config("spark.executor.cores", "5") \
    .config("spark.task.cpus", "1") \
    .master("spark://10.10.1.1:7077") \
    .getOrCreate()
# df = spark.read.csv(args.input)
df = spark.read.option("header",True).csv("hdfs://10.10.1.1:9000/input/export.csv")
df = df.orderBy(f.col("cca2").asc(),f.col("timestamp").asc())
# df.write.csv(args.output)
df.coalesce(1).write.option("header",True).csv("hdfs://10.10.1.1:9000/output/result_export.csv")
spark.stop()
