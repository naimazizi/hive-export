# hive-export

## Getting Started
1. Insert all table, columns and query that will be exported from Hive. List of queries and tables can be found in *query.tsv*, separated by tab (`\t`)
| table_name | columns        | query                               |
|------------|----------------|-------------------------------------|
| table1     | col1,col2,col3 | SELECT * FROM table1                |
| tabl2      | col1,col2      | SELECT * FROM table2 WHERE col1 > 1 |
2. Run the following command on terminal
`PYTHONPATH='.' luigi --module SqoopExport ListTable --local-scheduler`