import csv
import uuid
import psycopg2
from cassandra.cluster import Cluster

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname="postgres", 
    user="postgres", 
    password="Naren@03561", 
    host="localhost",
)
pg_cursor = pg_conn.cursor()

# Cassandra connection
cluster = Cluster(['127.0.0.1'],port=9042)
session = cluster.connect('my_keyspace')

# Extract data from PostgreSQL
pg_cursor.execute("SELECT id, username, email, created_at FROM users")
rows = pg_cursor.fetchall()

# Transform and load data into Cassandra
for row in rows:
    id = uuid.uuid4()  # Assuming you want to generate a new UUID
    username, email, created_at = row[1], row[2], row[3]
    session.execute(
        """
        INSERT INTO users (id, username, email, created_at) 
        VALUES (%s, %s, %s, %s)
        """,
        (id, username, email, created_at)
    )

# Close connections
pg_cursor.close()
pg_conn.close()
cluster.shutdown()
