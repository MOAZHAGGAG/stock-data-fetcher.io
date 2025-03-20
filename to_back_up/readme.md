Here is the explanation of the two SQL queries for deleting duplicates in Markdown format:

```markdown
# Deleting Duplicates in Trino and PostgreSQL

## Trino (Iceberg Table)

```sql
DELETE FROM iceberg.stock_db.daily_stock_prices 
WHERE (price, company_code, DATE(trade_date)) IN (
    SELECT price, company_code, DATE(trade_date)
    FROM iceberg.stock_db.daily_stock_prices
    GROUP BY price, company_code, DATE(trade_date)
    HAVING COUNT(*) > 1
);
```

### Explanation:
- This query deletes duplicate rows from the `daily_stock_prices` table in Trino (Iceberg).
- It identifies duplicates based on the combination of `price`, `company_code`, and the date part of `trade_date`.
- The `GROUP BY` and `HAVING COUNT(*) > 1` clause ensures only rows with duplicates are selected.
- The `DELETE` statement removes all rows that match the duplicate criteria.

---

## PostgreSQL

```sql
DELETE FROM daily_stock_prices
WHERE CTID IN (
    SELECT CTID FROM (
        SELECT CTID, 
               ROW_NUMBER() OVER (PARTITION BY price, company_code, DATE(trade_date) ORDER BY CTID) AS rn
        FROM daily_stock_prices
    ) t
    WHERE rn > 1
);
```

### Explanation:
- This query deletes duplicate rows from the `daily_stock_prices` table in PostgreSQL.
- PostgreSQL uses the `CTID` system column, which uniquely identifies a row in a table.
- The `ROW_NUMBER()` window function assigns a unique number to each row within a partition of `price`, `company_code`, and the date part of `trade_date`.
- Rows with `rn > 1` are considered duplicates and are deleted using the `CTID` identifier.

---

## Key Differences:
1. **Trino (Iceberg)**:
   - Uses a `GROUP BY` and `HAVING` clause to identify duplicates.
   - Deletes rows based on the combination of column values.

2. **PostgreSQL**:
   - Uses the `CTID` system column to uniquely identify rows.
   - Leverages the `ROW_NUMBER()` window function to identify duplicates within partitions.

Both queries effectively remove duplicates but are tailored to the specific database system's capabilities.
```
