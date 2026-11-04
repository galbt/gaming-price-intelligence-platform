from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.smoke.ping (
        id INT, message STRING
    ) USING iceberg
""")
spark.sql("DELETE FROM lakehouse.smoke.ping")
spark.sql("INSERT INTO lakehouse.smoke.ping VALUES (1, 'hello-iceberg')")

result = spark.sql("SELECT * FROM lakehouse.smoke.ping").collect()
assert len(result) == 1 and result[0]["message"] == "hello-iceberg", f"unexpected result: {result}"

print("SMOKE TEST PASSED")
spark.stop()
