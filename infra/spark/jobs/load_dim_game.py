import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

SCHEMA = StructType([
    StructField("game_key", StringType(), False),
    StructField("canonical_name", StringType(), False),
    StructField("coverage", StringType(), False),
    StructField("steam_appid", IntegerType(), True),
    StructField("ps_store_path", StringType(), True),
    StructField("igdb_id", IntegerType(), False),
    StructField("added_date", StringType(), False),
])


def main(seed_path: str) -> None:
    spark = SparkSession.builder.appName("load-dim-game").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dim")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.dim.dim_game (
            game_key STRING, canonical_name STRING, coverage STRING,
            steam_appid INT, ps_store_path STRING, igdb_id INT, added_date DATE
        ) USING iceberg
    """)

    # escape='"' matches Python's csv module, which escapes an embedded quote
    # by doubling it ("") rather than Spark's default backslash escape — without
    # this, a quoted title (e.g. containing a comma or an embedded quote) loads
    # with its quoting corrupted into the stored value. mode="FAILFAST" surfaces
    # a malformed row (bad int, bad date, empty/duplicate key) as an error
    # instead of silently writing NULL for it.
    df = spark.read.csv(seed_path, header=True, schema=SCHEMA, escape='"', mode="FAILFAST")
    df = df.withColumn("added_date", df["added_date"].cast("date"))
    df.createOrReplaceTempView("seed_source")
    spark.sql("INSERT OVERWRITE lakehouse.dim.dim_game SELECT * FROM seed_source")

    count = spark.sql("SELECT COUNT(*) AS n FROM lakehouse.dim.dim_game").collect()[0]["n"]
    print(f"LOAD DIM_GAME PASSED: {count} rows")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
