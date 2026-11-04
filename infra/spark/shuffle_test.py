from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("shuffle-test").getOrCreate()
sc = spark.sparkContext

result = sc.range(200000).repartition(8).map(lambda x: (x % 10, 1)).reduceByKey(lambda a, b: a + b).collect()
assert len(result) == 10, f"unexpected result: {result}"

print("SHUFFLE TEST PASSED")
spark.stop()
