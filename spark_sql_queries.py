"""
 Spark SQL Queries


Covers:
- Create SparkSession
- Load data into DataFrame
- Register DataFrame as SQL View
- Run SQL queries (SELECT, WHERE, GROUP BY, JOIN)
- Save query results to CSV


"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Spark SQL Queries - Day 3") \
        .master("local[*]") \
        .getOrCreate()

    # Sample data
    employees_data = [
        (1, "Aravind", "IT", 60000),
        (2, "Rahul", "HR", 45000),
        (3, "Priya", "IT", 70000),
        (4, "Sita", "Finance", 55000),
        (5, "John", "IT", 80000),
        (6, "Anita", "HR", 48000)
    ]

    departments_data = [
        ("IT", "Bangalore"),
        ("HR", "London"),
        ("Finance", "Delhi")
    ]

    #  Create DataFrames
    employees_df = spark.createDataFrame(employees_data, ["emp_id", "name", "dept", "salary"])
    departments_df = spark.createDataFrame(departments_data, ["dept", "location"])

    print("\n=== Employees DataFrame ===")
    employees_df.show()

    print("\n=== Departments DataFrame ===")
    departments_df.show()

    #  Register as SQL temporary views
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")

    #  Example SQL queries
    print("\n=== Query 1: Select all employees ===")
    spark.sql("SELECT * FROM employees").show()

    print("\n=== Query 2: Filter employees with salary > 50000 ===")
    spark.sql("SELECT name, dept, salary FROM employees WHERE salary > 50000").show()

    print("\n=== Query 3: Average salary per department ===")
    spark.sql("""
        SELECT dept, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY dept
        ORDER BY avg_salary DESC
    """).show()

    print("\n=== Query 4: Join employees with departments ===")
    spark.sql("""
        SELECT e.name, e.dept, e.salary, d.location
        FROM employees e
        JOIN departments d ON e.dept = d.dept
    """).show()

    #  Save result to CSV
    output_path = "output/spark_sql_results"
    joined_df = spark.sql("""
        SELECT e.name, e.dept, e.salary, d.location
        FROM employees e
        JOIN departments d ON e.dept = d.dept
    """)
    joined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"\n Results saved to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
