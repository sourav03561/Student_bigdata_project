import psycopg2
from cassandra.cluster import Cluster

# Establish the connection to PostgreSQL
pg_conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Naren@03561",
    host="localhost"
)

# Create a cursor object
cur = pg_conn.cursor()

# Execute a SQL query to fetch data from the students table
cur.execute("SELECT * FROM students")

# Fetch all rows from the executed query
rows = cur.fetchall()

# Print the column names (Optional)
colnames = [desc[0] for desc in cur.description]
print(colnames)

# Print out data types and values to debug (Optional)
for row in rows:
    print(f"Debug Row Data: {row}")
    print(f"Debug ID Column Type: {type(row[7])}, Value: {row[7]}")  # Check the type and value of ID

# Establish the connection to Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('my_keyspace')

# Prepare the insert statement for Cassandra
insert_query = session.prepare("""
INSERT INTO students (name, class_or_program, age, country, iq, cgpa, skill, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

# Iterate through the PostgreSQL data and insert it into Cassandra
for row in rows:
    try:
        # Extract data based on the column order
        name = row[0]
        class_or_program = row[1]
        age = row[2]
        country = row[3]
        iq = row[4]
        cgpa = row[5]
        skill = row[6]
        id = int(row[7])  # ID as an integer
        
        # Execute the insert statement
        session.execute(insert_query, (name, class_or_program, age, country, iq, cgpa, skill, id))
    except Exception as e:
        print(f"Error inserting row {row} - {e}")

# Close the cursor and connection to PostgreSQL
cur.close()
pg_conn.close()

# Close the Cassandra session
cluster.shutdown()
