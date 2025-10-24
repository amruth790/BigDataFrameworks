"""
 Spark SQL & Temporary Views


 this script demonstrates:
- Registering DataFrames as temporary views
- Running SQL queries using spark.sql()
- Using SQL joins, aggregations, sorting, and filtering
- Creating and querying global temporary views
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark_SQL") \
        .master("local[*]") \
        .getOrCreate()

    
    # Sample DataFrames
    
    employees = [
        (1, "Alice", "HR", 3000),
        (2, "Bob", "IT", 4000),
        (3, "Charlie", "IT", 3500),
        (4, "David", "Finance", 4500),
        (5, "Eva", "HR", 2800),
        (6, "Frank", "Legal", 5000)
    ]
    departments = [
        ("HR", "New York"),
        ("IT", "London"),
        ("Finance", "Toronto"),
        ("Legal", "Sydney")
    ]

    emp_df = spark.createDataFrame(employees, ["emp_id", "name", "dept", "salary"])
    dept_df = spark.createDataFrame(departments, ["dept", "location"])

    # Register as temp views
    emp_df.createOrReplaceTempView("employees")
    dept_df.createOrReplaceTempView("departments")

    
    # SQL Queries
    

    print("\n=== All Employees ===")
    spark.sql("SELECT * FROM employees").show()

    print("\n=== Filter: Employees with salary > 3500 ===")
    spark.sql("""
        SELECT name, dept, salary
        FROM employees
        WHERE salary > 3500
    """).show()

    print("\n=== Aggregation: Average Salary per Department ===")
    spark.sql("""
        SELECT dept, ROUND(AVG(salary), 2) AS avg_salary
        FROM employees
        GROUP BY dept
        ORDER BY avg_salary DESC
    """).show()

    print("\n=== Join Employees with Departments ===")
    spark.sql("""
        SELECT e.name, e.dept, e.salary, d.location
        FROM employees e
        JOIN departments d
        ON e.dept = d.dept
        ORDER BY e.salary DESC
    """).show()

    print("\n=== Top 2 highest salaries ===")
    spark.sql("""
        SELECT name, salary
        FROM employees
        ORDER BY salary DESC
        LIMIT 2
    """).show()

    
    # Global Temp View (optional)
    
    emp_df.createGlobalTempView("global_employees")

    print("\n=== Querying Global Temp View ===")
    spark.sql("""
        SELECT * FROM global_temp.global_employees WHERE dept = 'IT'
    """).show()

    spark.stop()

if __name__ == "__main__":
    main()
