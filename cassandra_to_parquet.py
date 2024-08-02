from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import pandas as pd

cluster = Cluster(['localhost'])  
session = cluster.connect('my_keyspace')

spark = SparkSession.builder.appName('practice').getOrCreate()

query = "SELECT * FROM student_data"
rows = session.execute(query)

data = [row._asdict() for row in rows]
df = pd.DataFrame(data)

df.to_parquet('output.parquet')

cluster.shutdown()