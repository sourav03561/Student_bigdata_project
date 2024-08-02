import psycopg2
import json
from decimal import Decimal

# Connect to the PostgreSQL database
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

# Get column names from the cursor description
colnames = [desc[0] for desc in cur.description]

# Convert Decimal to float for JSON serialization
def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type not serializable")

# Prepare the data in JSON format
data = []
for row in rows:
    row_dict = dict(zip(colnames, row))
    data.append(row_dict)

# Convert data to JSON and save it to a file
with open('students_data.json', 'w') as json_file:
    json.dump(data, json_file, indent=4, default=convert_decimal)

# Close the cursor and connection
cur.close()
pg_conn.close()
