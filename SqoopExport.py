import luigi
import luigi.configuration
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import logging
from datetime import date,datetime
from pyspark.sql import SparkSession
import logging
from configparser import SafeConfigParser
from typing import Dict
from itertools import chain

""" 
Retrieve Configuration from config.ini
"""
    
config = SafeConfigParser()
config.read('config.ini')

SPARK_SESSION_NAME= config.get('SPARK','SPARK_SESSION_NAME')
SPARK_DYNAMIC_ALLOCATION_ENABLED= config.get('SPARK','SPARK_DYNAMIC_ALLOCATION_ENABLED')
SPARK_EXECUTOR_INSTANCES= config.get('SPARK','SPARK_EXECUTOR_INSTANCES')
SPARK_EXECUTOR_CORES= config.get('SPARK','SPARK_EXECUTOR_CORES')
SPARK_EXECUTOR_MEMORY= config.get('SPARK','SPARK_EXECUTOR_MEMORY')
SPARK_EXECUTOR_MEMORYOVERHEAD= config.get('SPARK','SPARK_EXECUTOR_MEMORYOVERHEAD')

DATE_FORMAT = config.get('MISC','DATE_FORMAT')

SQOOP_JDBC_URL= '"{}"'.format(config.get('SQOOP','SQOOP_JDBC_URL'))
SQOOP_JDBC_USERNAME= config.get('SQOOP','SQOOP_JDBC_USERNAME')
SQOOP_JDBC_PASSWORD= config.get('SQOOP','SQOOP_JDBC_PASSWORD')
SQOOP_EXPORT_DIR= config.get('SQOOP','SQOOP_EXPORT_DIR')
SQOOP_NUM_MAPPER= config.get('SQOOP','SQOOP_NUM_MAPPER')
SQOOP_UPDATE_MODE= config.get('SQOOP','SQOOP_UPDATE_MODE')
SQOOP_FIELD_TERMINATED = config.get('SQOOP','SQOOP_FIELD_TERMINATED')

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

today = date.today().strftime(DATE_FORMAT)

class SparkSingleton:
    __instance = None
    __tempTime = None
    
    @staticmethod 
    def getInstance()->SparkSession:
        if SparkSingleton.__instance == None:
            SparkSingleton()
        return SparkSingleton.__instance

    def __init__(self):
        self.create_spark_session()

    @staticmethod
    def create_spark_session() -> None:
        try:
            SparkSingleton.__instance = SparkSession.builder \
                .appName(SPARK_SESSION_NAME) \
                .config('spark.dynamicAllocation.enabled', SPARK_DYNAMIC_ALLOCATION_ENABLED) \
                .config('spark.executor.instances', SPARK_EXECUTOR_INSTANCES) \
                .config('spark.executor.cores', SPARK_EXECUTOR_CORES) \
                .config('spark.executor.memory', SPARK_EXECUTOR_MEMORY) \
                .config('spark.executor.memoryOverhead', SPARK_EXECUTOR_MEMORYOVERHEAD) \
                .enableHiveSupport() \
                .getOrCreate()
            SparkSingleton.__tempTime = datetime.utcnow()
            logging.info('{} - Created Spark Session'.format(SPARK_SESSION_NAME))
        except Exception as e:
            logging.error('Failed to create Spark Session',exc_info=True)
        
    @staticmethod
    def stop_spark_session() -> None:
        SparkSingleton.__instance.stop()
        SparkSingleton.__instance = None
        logging.info('{} - Stopped Spark Session, time elapsed: {}'.format(
            SPARK_SESSION_NAME, (datetime.utcnow()-SparkSingleton.__tempTime).total_seconds()))
      

'''
First task - Read query from file on defined path

example file:
table_1     col1,col2,col3      SELECT * FROM ABC
table_2     col1,col2,col3      SELECT * FROM BCD
'''

class ReadListQuery(luigi.Task):
    path = luigi.Parameter()

    def requires(self) -> None:
        return []

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget("logs/{}_1_queue_query.csv".format(today))

    def run(self) -> None :
        logging.info("Starting to read all queries")
        with open(self.path,'r') as input, self.output().open('w') as output:
            for line_num,line in enumerate(input, start=1):
                lines = line.split('\t')
                if(len(lines)) == 3:
                    output.write("{}\n".format(line))
                else:
                    logging.error('Unexpected input on line number {}. "{}"'.format(line_num, line))
    
'''
Second Task - Create temporary table to save the result of query, since sqoop can not use custom query to export hive data
'''

class CreateTempTable(luigi.Task):
    path = luigi.Parameter()
    spark = None
    __tempTime = None

    def requires(self) -> None:
        return [ReadListQuery(self.path)]

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget('logs/{}_2_queue_create_temp_table.csv'.format(today))


    def run(self) -> None:
        self.spark = SparkSingleton.getInstance()

        with self.input()[0].open() as input, self.output().open('w') as output:
            for line in input:
                try:
                    lines = line.split("\t")
                    self.spark.sql("DROP TABLE IF EXISTS {}".format(lines[0]))
                    create_query = 'CREATE TABLE {} ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t" LINES TERMINATED BY "\n" STORED AS TEXTFILE AS {} '.format(lines[0],lines[2])
                    self.spark.sql(create_query)
                    logging.info('Created Table "{}" with Query "{}"'.format(lines[0],lines[2]))
                    output.write('{}\t{}\tCREATED\n'.format(lines[0],lines[1]))
                except Exception as e:
                    logging.error('Failed to create table "{}" with Query "{}"\n'.format(lines[0],lines[2]),exc_info=True)
        
        SparkSingleton.stop_spark_session();

'''
Third Task - Insert data from hive table into database using sqoop
'''

class InsertToDatabase(luigi.Task):
    path = luigi.Parameter()
    cmd = luigi.configuration.get_config().get('sqoop','command','sqoop')
    arglist = [
        '--connect',SQOOP_JDBC_URL,
        '--username', SQOOP_JDBC_USERNAME,
        '--password', SQOOP_JDBC_PASSWORD,
        '--num-mappers', SQOOP_NUM_MAPPER,
        '--update-mode',SQOOP_UPDATE_MODE,
        '--fields-terminated-by', SQOOP_FIELD_TERMINATED
    ]
    

    def requires(self) -> None:
        return [CreateTempTable(self.path)]

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget('logs/{}_3_queue_insert_database.csv'.format(today))

    def run(self) -> None:
        with self.input()[0].open() as input, self.output().open('w') as output:
            for line in input:
                lines = line.split('\t')
                table = lines[0]
                columns = lines[1]
                try:
                    sqoop_arglist = [self.cmd, 'export']
                    sqoop_arglist.extend(self.arglist)
                    sqoop_arglist.extend(['--export-dir','{}/{}'.format(SQOOP_EXPORT_DIR,table)])
                    sqoop_arglist.extend(['--table',table])
                    sqoop_arglist.extend(['--columns','"{}"'.format(columns)])

                    logging.info("Start inserting data to table {}".format(table))
                    luigi.contrib.hadoop.run_and_track_hadoop_job(arglist=sqoop_arglist)
                    output.write('{}\tINSERTED\n'.format(table))
                    logging.info("Insert data into table {} is completed".find(table))
                except Exception as identifier:
                    logging.error("Error in inserting data to table {}".format(table),exc_info=True)