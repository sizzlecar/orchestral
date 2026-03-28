---
name: db-query
description: "Query MySQL databases, summarize results, and export to CSV/Excel. Use when the user asks to check data, run SQL queries, export tables, or generate data reports from a database."
compatibility: "Requires mysql CLI client. Environment variables: DB_HOST, DB_USER, DB_PASS, DB_NAME (optional: DB_PORT defaults to 3306)."
metadata:
  author: orchestral
  version: "0.1.0"
---

# Database Query Skill

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_HOST` | Yes | — | MySQL server hostname or IP |
| `DB_USER` | Yes | — | Database username |
| `DB_PASS` | Yes | — | Database password |
| `DB_NAME` | No | — | Default database name |
| `DB_PORT` | No | 3306 | MySQL server port |

## Workflow

1. **Verify connectivity** before running queries:
   ```
   mysql -h "$DB_HOST" -P "${DB_PORT:-3306}" -u "$DB_USER" -p"$DB_PASS" -e "SELECT 1" 2>&1
   ```

2. **Run queries** using the mysql CLI with these flags for clean output:
   ```
   mysql -h "$DB_HOST" -P "${DB_PORT:-3306}" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "YOUR SQL" --batch --skip-column-names
   ```

3. **For tabular output** (human-readable), omit `--skip-column-names`:
   ```
   mysql -h "$DB_HOST" -P "${DB_PORT:-3306}" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "YOUR SQL" --table
   ```

4. **Export to CSV**:
   ```
   mysql -h "$DB_HOST" -P "${DB_PORT:-3306}" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "YOUR SQL" --batch | tr '\t' ',' > output.csv
   ```

## Safety Rules

- NEVER run `DROP`, `TRUNCATE`, `DELETE`, or `UPDATE` without explicit user confirmation
- NEVER modify database schema (`ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`) without approval
- Always use `LIMIT` for exploratory queries to avoid fetching millions of rows
- Use `--batch` mode for programmatic output, `--table` for human display
- Mask passwords in any output shown to the user — never echo `$DB_PASS`

## Query Patterns

- **Explore schema**: `SHOW TABLES`, `DESCRIBE table_name`, `SHOW CREATE TABLE table_name`
- **Row counts**: `SELECT COUNT(*) FROM table_name`
- **Sample data**: `SELECT * FROM table_name LIMIT 10`
- **Aggregations**: Use `GROUP BY` with `COUNT`, `SUM`, `AVG` for summaries
- **Date filtering**: `WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`
