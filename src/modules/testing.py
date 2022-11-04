import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F


def create_rawlayer():
    spark = SparkSession.builder \
        .master("local") \
        .appName("demo") \
        .enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3') \
        .getOrCreate()

    sfOptions = {
        "sfURL": "https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "kaveri_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "KAVERI_WH"
    }

    """# #Log_Details(raw_layer)"""

    """# Read CSV File and Write to Table"""""

    df = spark.read.option("delimiter", " ").csv("C:\\Users\\kaverip\\Downloads\\299999.text")
    # df = spark.read.option("delimiter"," ").csv("s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")

    df.show(truncate=False)

    """# Giving col names to each columns"""

    # import pyspark.sql.functions as F
    df_col = (df.select(
        F.monotonically_increasing_id().alias('row_id'),
        F.col("_c0").alias("client_ip"),
        F.split(F.col("_c3"), " ").getItem(0).alias("datetime"),
        F.split(F.col("_c5"), " ").getItem(0).alias("method"),
        F.split(F.col("_c5"), " ").getItem(1).alias("request"),
        F.col("_c6").alias("status_code"),
        F.col("_c7").alias("size"),
        F.col("_c8").alias("referrer"),
        F.col("_c9").alias("user_agent")
    ))

    df_col.printSchema()

    df_col.show(truncate=False)

    # save raw data in s3
    # df_col.write.mode("overwrite").format('csv').option("header",True).save("s3://databrickskaveri/final_layer/Raw/raw_log_details")

    # RAW_DATA HIVE TABLE
    df_col.write.mode("overwrite").saveAsTable("raw_log_details")
    df_log = spark.sql("select * from raw_log_details")
    df_log.show()

    df_log = spark.sql("select count(*) from raw_log_details").show()
    # df_log.write.format("snowflake")\
    #     .option("dbtable", "KAVERI_RAW_LOG_DETAILS").mode("overwrite")\
    #     .save()

if __name__ == '__main__':
    create_rawlayer()
