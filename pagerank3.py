from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("part2") \
    .config("spark.driver.memory", "30g") \
    .config("spark.executor.memory", "30g") \
    .config("spark.executor.cores", "5") \
    .config("spark.task.cpus", "1") \
    .master("spark://10.10.1.1:7077") \
    .getOrCreate()

links = spark.read.options(comment="#", delimiter="\t").csv("hdfs://10.10.1.1:9000/input/web-BerkStan.txt").toDF("page", "neighbor").repartition(10, "page").cache()
ranks = links.select("page").distinct().withColumn("rank", lit(1)).repartition(10, "page")
linkcount = links.groupBy("page").count().repartition(10, "page").cache()

for i in range(0, 10):
    contribs = links.join(linkcount, ["page"], "inner").join(ranks, ["page"], "inner") \
                    .withColumn("contrib", col('rank') / col('count')).select("neighbor", "contrib") \
                    .groupBy("neighbor").sum("contrib").toDF("page", "contribs")
    ranks = contribs.withColumn("rank", 0.15 + 0.85 * col('contribs')).select("page", "rank").repartition(10, "page")

ranks.write.option("header",True).csv("hdfs://10.10.1.1:9000/output/pagerank_wiki_3")
