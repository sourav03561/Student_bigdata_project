from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('practice').getOrCreate()

parquet_file_path = "output.parquet"
df = spark.read.parquet(parquet_file_path)

df.show(5)