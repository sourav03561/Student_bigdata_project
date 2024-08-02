from cassandra.cluster import Cluster
import json
from cassandra.query import BatchStatement, SimpleStatement


batch = BatchStatement()

# Connect to Cassandra
cluster = Cluster(['localhost'])  # Replace with your Cassandra nodes
session = cluster.connect('my_keyspace')  # Replace with your keyspace

# Create table if it doesn't exist
session.execute("""
CREATE TABLE IF NOT EXISTS student_data (
    id BIGINT PRIMARY KEY,
    name TEXT,
    class_or_program TEXT,
    age INT,
    country TEXT,
    iq INT,
    cgpa FLOAT,
    skill TEXT
)
""")

# Load JSON data
with open('students_data.json', 'r') as file:
    data = json.load(file)

# Prepare the insert statement
insert_stmt = """
INSERT INTO student_data (id, name, class_or_program, age, country, iq, cgpa, skill)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Insert data into Cassandra
for entry in data:
    # Ensure that 'id' is an integer
    id = int(entry['id'])
    name = entry['name']
    class_or_program = entry['class_or_program']
    age = entry['age']
    country = entry['country']
    iq = entry['iq']
    cgpa = entry['cgpa']
    skill = entry['skill']
    batch.add(insert_stmt, (id, name, class_or_program, age, country, iq, cgpa, skill))

    # Execute the batch
    if len(batch) >= 100:  # Adjust the batch size as needed
        session.execute(batch)
        batch.clear()
    # Execute the INSERT query
print("Data inserted successfully!")
