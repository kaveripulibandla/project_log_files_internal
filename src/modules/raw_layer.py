# import sys
import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace


def create_rawlayer():

    spark = SparkSession.builder.enableHiveSupport()\
        .config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()

    """# #Log_Details(raw_layer)"""

    """# Read CSV File and Write to Table"""""
    df = spark.read.option("delimiter"," ").csv("C:\\Users\\kaverip\\Downloads\\project-demo-processed-input.txt")

    # .csv("s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")

    df.show(truncate = False)

    """# Giving col names to each columns"""

    df_col = (df.select(
        F.monotonically_increasing_id().alias('row_id'),
        F.col("_c0").alias("client_ip"),
        F.split(F.col("_c3")," ").getItem(0).alias("datetime"),
        F.split(F.col("_c5"), " ").getItem(0).alias("method"),
        F.split(F.col("_c5"), " ").getItem(1).alias("request"),
        F.col("_c6").alias("status_code"),
        F.col("_c7").alias("size"),
        F.col("_c8").alias("referrer"),
        F.col("_c9").alias("user_agent")
        ))

    df_col.printSchema()
    df_col.show(truncate = False)

    df_col1 = df_col.withColumn('datetime', regexp_replace('datetime', '\[|\]|', '')) \
        .dropDuplicates(["client_ip", "datetime", "method"]) \
        .drop("row_id") \
        .withColumn('row_id', monotonically_increasing_id()) \
        .select('row_id', 'client_ip', 'datetime', 'method', 'request', 'status_code','size','referrer','user_agent')

    df_col1.show(truncate=False)

    df_col1.coalesce(1).write.mode("overwrite").format('csv')\
        .option("header", True).save("C:\\project_log_files_internal\\src\\internal_files\\raw_log_file.csv")

    sfOptions = {
        "sfURL": r"https://hisswyy-qi52071.snowflakecomputing.com/",
        "sfAccount": "su57550",
        "sfUser": "sunil",
        "sfPassword": "Cloud@123",
        "sfDatabase": "KAVERI_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }
    df_col1.coalesce(1).write.format("snowflake").options(**sfOptions)\
        .option("dbtable","{}".format(r"raw_log_details")).mode("overwrite").options(header=True).save()

    # SnowflakeHelper().save_df_to_snowflake(df_col, env.sf_raw_table)

    # save raw data in s3
    # df_col.write.mode("overwrite").format('csv').option("header",True).save("s3://databrickskaveri/final_layer/Raw/raw_log_details")

    """"" # RAW_DATA HIVE TABLE """""
    df_col1.coalesce(1).write.mode("overwrite").saveAsTable("raw_log_details")
    df_log = spark.sql("select * from raw_log_details")
    df_log.show()

    df_log = spark.sql("select count(*) from raw_log_details")
    df_log.show()

if __name__ == '__main__':
    create_rawlayer()
