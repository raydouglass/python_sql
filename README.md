# Python SQL

This is a naive implementation of an SQL database in pure Python with no external libraries.

I made this just to experiment with SQL and databases. It's not meant for production use.

It's made of three components:
* B+ Tree for indexes
* An [LL Parser](https://en.wikipedia.org/wiki/LL_parser) for a subset of SQL
* In-memory data structure for table data


## Support

* `CREATE TABLE`
* `INSERT`
* `SELECT`
* `UPDATE`
* Cross `JOIN`
* Primary key index

### Caveats

Columns must be referred by table and name such as `table.col_name`. `table.col_name AS <name>` is supported and optional.

You must explictly `JOIN` tables beyond the first.

```sql
//Incorrect
SELECT ... FROM table_1, table_2
//Correct
SELECT ... FROM table_1 JOIN table_2
```
