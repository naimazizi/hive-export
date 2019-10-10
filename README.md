# HIVE-Export

## Introduction
Combination between Apache Spark and Sqoop to extract data from Hive table into relational database (MySQL in this source code, but can use another rdbms as long as [supported](http://sqoop.apache.org/docs/1.4.0-incubating/SqoopUserGuide.html#id1773570) by hive), integrated with pipeline using luigi.

## Getting Started

1. Insert all table, columns and query that will be exported from Hive. List of queries and tables can be found in *query.tsv*, separated by tab (`\t`)

    | table_name | columns        | query                               |
    |------------|----------------|-------------------------------------|
    | table1     | col1,col2,col3 | SELECT * FROM table1                |
    | tabl2      | col1,col2      | SELECT * FROM table2 WHERE col1 > 1 |

2. Create log for luigi central scheduler

    `mkdir /tmp/luigid`    
    `touch /tmp/luigid/luigi-server.log`

3. Start luigi central scheduler

    `luigid --logdir=/tmp/luigid`

    Luigi central scheduler can be accessed at [http://localhost:8082](http://localhost:8082)

4. Run the following command on terminal 

    `luigi --module HiveExport InsertToDatabase --local-scheduler --path query.tsv` 

    **Need to add current module to PYTHONPATH**

## Development Environment *(optional)*

If you don't have any access to hadoop environment that integrated with spark and hive, you can setup your own development environment using vagrant (follow this [link](https://github.com/martinprobson/vagrant-hadoop-hive-spark)).

## Troubleshooting
* **key not found: _PYSPARK_DRIVER_CALLBACK_HOST**

    Add following path to PYTHONPATH (append `~/.bashrc`). Replace py4j version that match your Apache spark (py4j is already bundled in Apache spark installation folder).

        export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
        export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH
        export PYTHONPATH=/vagrant/hive-export/:$PYTHONPATH  

        
