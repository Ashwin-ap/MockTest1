1. Write a PySpark code to read a CSV file named "employees.csv" containing the following columns: "employee_id", "name", "age", "department". Display the top 10 records from the DataFrame.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Read CSV").getOrCreate()
df = spark.read.csv("employees.csv", header=True, inferSchema=True)
df.show(10)

2.  Given a PySpark DataFrame named "sales_data" with columns "product_name" and "revenue", write a code to calculate the total revenue for each product and display the result in descending order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Calculate Total Revenue ").getOrCreate()

sales_data = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
total_rev = sales_data.groupBy("product_name").agg(sum("revenue").alias("total_revenue"))
sorted_df = total_rev.orderBy(desc("total_revenue"))
sorted_df.show()

3. Write a PySpark code to read a JSON file named "students.json" containing student records with the following schema: "name" (string), "age" (integer), "grade" (string). Filter the DataFrame to include only students whose age is greater than 18.

from pyspark.sql import *
spark = SparkSession.builder.appName("Read JSON").getOrCreate()
df = spark.read.json("students.json")
filtered_df = df.filter(df.age > 18)
filtered_df.show()

4. Consider a PySpark DataFrame named "transactions" with columns "transaction_id", "user_id", and "amount". Write a code to calculate the average transaction amount for each user and display the result.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Calculate Average Transaction Amount").getOrCreate()
# Assuming you already have a DataFrame named "transactions"

average_amount_df = transactions.groupBy("user_id").agg(avg("amount").alias("average_amount"))
average_amount_df.show()

5. Given a PySpark DataFrame named "logs" with columns "timestamp" (timestamp) and "event" (string), write a code to count the number of events that occurred in each hour and display the result sorted by the hour.

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("Count Events by Hour").getOrCreate()

# Assuming you already have a DataFrame named "logs"
logs_with_hour = logs.withColumn("hour", hour("timestamp")
event_count_df = logs_with_hour.groupBy("hour").agg(count("event").alias("event_count"))
sorted_df = event_count_df.orderBy("hour")
sorted_df.show()

6.  Retrieve all the customers from the "Customers" table whose age is greater than 25 and have made at least one purchase.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Retrieve Customers").getOrCreate()
# Assuming you have already connected to the database and have a DataFrame named "Customers"
Customers.createOrReplaceTempView("Customers")
query = """
SELECT *
FROM Customers
WHERE age > 25
  AND customer_id IN (SELECT DISTINCT customer_id FROM Purchases)
"""
result = spark.sql(query)
result.show()

7. Find the total number of orders placed by each customer and display the results in descending order of the number of orders.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder.appName("Count Orders").getOrCreate()

# Assuming you have a DataFrame named "Orders" representing the orders table
order_count_df = Orders.groupBy("customer_id").agg(count("*").alias("order_count"))
sorted_df = order_count_df.orderBy("order_count", ascending=False)
sorted_df.show()

8. Retrieve the names of all products that are currently out of stock from the "Products" table.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Retrieve Out of Stock Products").getOrCreate()

# Assuming you have already connected to the database and have a DataFrame named "Products"
Products.createOrReplaceTempView("Products")
query = """
SELECT product_name
FROM Products
WHERE stock_quantity = 0
"""
result = spark.sql(query)
result.show()

9. Calculate the average price of all products in each category and display the results along with the category name.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
spark = SparkSession.builder.appName("Average Price by Category").getOrCreate()

# Assuming you have a DataFrame named "Products" representing the products table
average_price_df = Products.groupBy("category").agg(avg("price").alias("average_price"))
average_price_df.show()

10. Retrieve the top 5 customers who have spent the highest total amount on purchases.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Top Customers").getOrCreate()

# Assuming you have already connected to the database and have a DataFrame named "Purchases"
Purchases.createOrReplaceTempView("Purchases")
query = """
SELECT customer_id, SUM(amount) AS total_amount
FROM Purchases
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 5
"""
result = spark.sql(query)
result.show()

