"""
DataFrame Joins & Window Functions


What this script demonstrates:
- Inner / Left / Right / Full joins between DataFrames
- Window functions: rank, dense_rank, row_number, lag, lead, max over partition
- Practical examples and saving results to CSV for inspection


"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pathlib import Path

def main():
    spark = SparkSession.builder \
        .appName("Joins_Windows") \
        .master("local[*]") \
        .getOrCreate()

    # Ensure output folder
    out_dir = Path("output/joins_windows")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Sample data
    employees = [
        (1, "Alice", "HR", 3000),
        (2, "Bob", "IT", 4000),
        (3, "Charlie", "IT", 3500),
        (4, "David", "Finance", 4500),
        (5, "Eva", "HR", 2800),
        (6, "Frank", "Legal", 5000)  # dept without department metadata
    ]
    departments = [
        ("HR", "New York"),
        ("IT", "London"),
        ("Finance", "Toronto")
        # Note: "Legal" intentionally missing to show unmatched rows
    ]

    # Create DataFrames
    emp_df = spark.createDataFrame(employees, ["emp_id", "name", "dept", "salary"])
    dept_df = spark.createDataFrame(departments, ["dept", "location"])

    print("\n=== Employees ===")
    emp_df.show(truncate=False)

    print("\n=== Departments ===")
    dept_df.show(truncate=False)

    
    # JOINS
    
    print("\n=== Inner Join ===")
    inner = emp_df.join(dept_df, on="dept", how="inner")
    inner.show(truncate=False)
    inner.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "inner_join")

    print("\n=== Left Join ===")
    left = emp_df.join(dept_df, on="dept", how="left")
    left.show(truncate=False)
    left.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "left_join")

    print("\n=== Full Outer Join ===")
    full = emp_df.join(dept_df, on="dept", how="outer")
    full.show(truncate=False)
    full.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "full_join")

    
    # WINDOW FUNCTIONS
    
    # Partition by department and order by salary descending
    window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))

    print("\n=== Rank, Dense Rank, Row Number by Dept (salary desc) ===")
    ranked = emp_df.withColumn("rank", F.rank().over(window_spec)) \
                   .withColumn("dense_rank", F.dense_rank().over(window_spec)) \
                   .withColumn("row_number", F.row_number().over(window_spec))
    ranked.show(truncate=False)
    ranked.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "ranked_by_dept")

    print("\n=== Max Salary per Dept (window) ===")
    max_salary = emp_df.withColumn("max_salary_dept", F.max("salary").over(Window.partitionBy("dept")))
    max_salary.show(truncate=False)
    max_salary.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "max_salary_dept")

    print("\n=== Lag & Lead (previous and next salary within dept) ===")
    lag_lead = emp_df.withColumn("prev_salary", F.lag("salary", 1).over(window_spec)) \
                     .withColumn("next_salary", F.lead("salary", 1).over(window_spec))
    lag_lead.show(truncate=False)
    lag_lead.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "lag_lead")

    
    # Practical examples
    
    print("\n=== Top 2 paid employees per department ===")
    top2_window = Window.partitionBy("dept").orderBy(F.desc("salary"))
    top2 = emp_df.withColumn("rn", F.row_number().over(top2_window)).filter(F.col("rn") <= 2).drop("rn")
    top2.show(truncate=False)
    top2.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir / "top2_per_dept")

    print(f"\n✅ Outputs saved to folder: {out_dir.resolve()}")
    spark.stop()

if __name__ == "__main__":
    main()
