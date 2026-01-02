#!/usr/bin/env python
"""
WaveQL Demo Script

Demonstrates core functionality with a local CSV file.
"""

import waveql
import tempfile
import csv
from pathlib import Path


def create_sample_data():
    """Create sample CSV for demo."""
    temp_dir = Path(tempfile.mkdtemp())
    csv_path = temp_dir / "employees.csv"
    
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "department", "salary", "active"])
        writer.writerow([1, "Alice Johnson", "Engineering", 95000, True])
        writer.writerow([2, "Bob Smith", "Marketing", 75000, True])
        writer.writerow([3, "Charlie Brown", "Engineering", 85000, True])
        writer.writerow([4, "Diana Ross", "Sales", 80000, False])
        writer.writerow([5, "Eve Wilson", "Engineering", 105000, True])
    
    return str(csv_path)


def main():
    print("=" * 60)
    print("WaveQL Demo - Query CSV with SQL")
    print("=" * 60)
    print()
    
    # Create sample data
    csv_path = create_sample_data()
    print(f"Created sample CSV: {csv_path}")
    print()
    
    # Connect to CSV file
    conn = waveql.connect(f"file://{csv_path}")
    cursor = conn.cursor()
    
    # Demo 1: Simple SELECT
    print("1. SELECT all employees:")
    print("-" * 40)
    cursor.execute("SELECT * FROM employees")
    for row in cursor:
        print(row)
    print()
    
    # Demo 2: SELECT with columns
    print("2. SELECT specific columns:")
    print("-" * 40)
    cursor.execute("SELECT name, department FROM employees")
    for row in cursor:
        print(row)
    print()
    
    # Demo 3: SELECT with WHERE (predicate pushdown)
    print("3. SELECT with predicate pushdown (Engineering only):")
    print("-" * 40)
    cursor.execute("SELECT name, salary FROM employees WHERE department = 'Engineering'")
    for row in cursor:
        print(row)
    print()
    
    # Demo 4: SELECT with LIMIT
    print("4. SELECT with LIMIT:")
    print("-" * 40)
    cursor.execute("SELECT name FROM employees LIMIT 3")
    for row in cursor:
        print(row)
    print()
    
    # Demo 5: Convert to Pandas
    print("5. Convert to Pandas DataFrame:")
    print("-" * 40)
    cursor.execute("SELECT * FROM employees WHERE salary > 80000")
    df = cursor.to_df()
    print(df)
    print()
    
    # Demo 6: Arrow Table (zero-copy)
    print("6. Get as Arrow Table:")
    print("-" * 40)
    cursor.execute("SELECT name, salary FROM employees")
    arrow_table = cursor.to_arrow()
    print(f"Arrow Table: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    print(f"Schema: {arrow_table.schema}")
    print()
    
    # Cleanup
    conn.close()
    Path(csv_path).unlink()
    Path(csv_path).parent.rmdir()
    
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
