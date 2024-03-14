import sqlite3
import csv

connection = sqlite3.connect('Data Engineer_ETL Assignment.db')
cursor = connection.cursor()
query = """
SELECT c.customer_id, c.age, i.item_name, SUM(CAST(o.quantity AS INT)) AS quantity
FROM Orders o
JOIN Sales s ON o.sales_id = s.sales_id
JOIN Customers c ON s.customer_id = c.customer_id
JOIN Items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
GROUP BY c.customer_id,age, i.item_id
HAVING quantity > 0
"""

cursor.execute(query)
with open('output_sql.csv', 'w', newline='') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['customer', 'age', 'item', 'quantity'])
    writer.writerows(cursor.fetchall())
connection.close()
