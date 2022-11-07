import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
# from helpers.snowflake_helper import SnowflakeHelper

# import env

def log_cleansed_layer():

    spark = SparkSession.builder.enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    spark

    """# #data reading from Log_Details(raw_layer)"""

    # Read CSV File and Write to Table
    df = spark.read.option("header", True)\
        .csv("C:\\project_log_files_internal\\src\\internal_files\\raw_log_file")

    # df = spark.read.option("delimiter", " ").csv("s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")

    df.show(truncate=False)

    """## ##Log_details(Cleansed_layer)"""

    # Removing the [] in datetime column by using regex replace func
    df_clean = df.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))
    df_clean.show()

    df_date = df_clean.withColumn("datetime", to_timestamp("datetime", "dd/MMM/yyyy:HH:mm:ss")).withColumn('datetime',
                                                                                                           date_format(
                                                                                                               col("datetime"),
                                                                                                               "MM/dd/yyyy HH:mm:ss"))


    df_date.show(truncate = False)
    df_date.printSchema()

    # change the datatype of statuscode in to int
    df_sc_int = df_date.withColumn("status_code", col("status_code").cast("int")).withColumn("row_id", col("row_id").cast("int"))
    df_sc_int.show()
    df_sc_int.printSchema()

    # df_date1 = df_date.select(col("*"),date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss").alias("datetime_format"))

    # Applying the condition to the above df

    cleaned_df = df_sc_int.withColumn("referer_present(YorN)",
                                    when(col("referrer") == "-", "N") \
                                    .otherwise("Y"))

    cleaned_df.show()

    cleansed_data = cleaned_df.drop("referrer")
    cleansed_data.show(truncate=False)

    cleansed_data.write.mode("overwrite").format('csv').option("header", True).save(
        "C:\\project_log_files_internal\\src\\internal_files\\cleanse_log_file")

    # SnowflakeHelper().save_df_to_snowflake(cleansed_data, env.sf_cleansed_table)

    # Save cleansed_data in s3
    # cleansed_data.mode("overwrite").format('csv').option("header", True).save(
    #     "s3://databrickskaveri/final_layer/cleansed/cleanse_log_details")

    # CLEANSED DATA IN HIVE TABLE
    cleansed_data.write.mode("overwrite").saveAsTable("cleanse_log_details")
    cleansed_hive = spark.sql("select * from cleanse_log_details")
    cleansed_hive.show(truncate = False)

    cleansed_hive = spark.sql("select count(*) from cleanse_log_details").show()

    sfOptions = {
        "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfAccount": "tm57257",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "KAVERI_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    cleansed_data.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                 "{}".format(r"kaveri_cleansed_log_details")).mode(
        "overwrite").options(header=True).save()
    spark.stop

if __name__ == '__main__':
    log_cleansed_layer()
    # SnowflakeHelper().save_df_to_snowflake(obj, env.sf_cleansed_table)


