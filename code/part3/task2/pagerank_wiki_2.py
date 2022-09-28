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

links = spark.read.options(comment="#", delimiter="\t").csv("hdfs://10.10.1.1:9000/input/enwiki-pages-articles/*").toDF("page", "neighbor").repartition(10, "page")
ranks = links.select("page").distinct().withColumn("rank", lit(1))
linkcount = links.groupBy("page").count()

for i in range(0, 10):
    contribs = links.join(linkcount, ["page"], "inner").join(ranks, ["page"], "inner") \
                    .withColumn("contrib", col('rank') / col('count')).select("neighbor", "contrib") \
                    .repartition(10, "neighbor").groupBy("neighbor").sum("contrib").toDF("page", "contribs")
    ranks = contribs.withColumn("rank", 0.15 + 0.85 * col('contribs')).select("page", "rank")

ranks.write.option("header",True).csv("hdfs://10.10.1.1:9000/output/pagerank_wiki_2")
ranks.agg(max(col("rank"))).show()
ranks.count()
links.count()
ranks.show()
spark.stop()