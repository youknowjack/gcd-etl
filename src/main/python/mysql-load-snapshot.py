import mysql.connector

# Create a connection object to the MySQL database
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='password',
    database='mydatabase'
)

# Create a cursor object from the connection object
cursor = connection.cursor()

# Execute the MySQL query
cursor.execute('SELECT * FROM mytable')

# Fetch the results of the query
results = cursor.fetchall()

# Close the connection and cursor objects
connection.close()
cursor.close()

# Print the results of the query
for row in results:
    print(row)

