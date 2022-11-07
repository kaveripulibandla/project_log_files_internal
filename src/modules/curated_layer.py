import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
# from helpers.snowflake_helper import SnowflakeHelper


# split the date and hours from datetime col

def spliting_date(val):
    return val.split(":")[0]

def log_curated_layer():

    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    spark
    """# #reading the data from Log_Details(cleanse_layer)"""

    # Read CSV File and Write to Table
    df_curated = spark.read.option("header", True) \
        .csv(
        "C:\\project_log_files_internal\\src\\internal_files\\cleanse_log_file\\clean_data.csv")

    #     "s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")
    df_curated.show(truncate=False)

    """1.Log_details(Curated_layer)"""

    """## #i)Remove any special characters in the request column(% ,- ? =)"""

    curated_data = df_curated.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    curated_data.show(truncate=False)

    """## #ii) Replace null with NA"""

    curated_data.na.fill("Nan").show(truncate=False)

    """# iii)convert the column size bytes in to kb"""

    def convert_to_kb(val):
        return str(int(val) / (10 ** 3)) + " KB"

    convert_to_kb_udf = F.udf(lambda x: convert_to_kb(x), StringType())
    final_curated= curated_data.withColumn("size", convert_to_kb_udf(col("size")))

    # final_curated = curated_data.withColumn("size_in_kb", round(col("size") / 1024, 2))
    final_curated.show(truncate = False)

    final_curated.write.mode("overwrite").format('csv').option("header", True).save(
        "C:\\project_log_files_internal\\src\\internal_files\\curate_log_file")
    # SnowflakeHelper().save_df_to_snowflake(final_curated, env.sf_curated_table)
    sfOptions = {
        "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfAccount": "tm57257",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "KAVERI_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "KAVERI_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    final_curated.write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(r"kaveri_curated_log_details")).mode("overwrite").options(header=True).save()

    # save curated data in s3
    # curated_data1.write.mode("overwrite").format('csv').option("header", True).save("s3://databrickskaveri/final_layer/curated/curate_log_details")

    # CURATED_HIVE TABLE
    #
    final_curated.write.mode("overwrite").saveAsTable("curate_log_details")
    curated_hive = spark.sql("select * from curate_log_details")
    curated_hive.show(truncate = False)

    """# #Data aggregation and reporting

    # #Log_agg_per_device
    """

    df_grp_get = curated_hive.groupBy("method").agg(count("method").alias("method_count"))
    df_grp_get.show()

    spliting_date_udf = udf(lambda x: spliting_date(x), StringType())

    apply_cond = lambda x: sum(when(x, 1).otherwise(0))

    log_agg_per_device = curated_hive.withColumn("day_hour", spliting_date_udf(col("datetime"))).groupBy("day_hour",
                                                                                                        "client_ip") \
        .agg(apply_cond(col('method') == "GET").alias("no_get"), \
             apply_cond(col('method') == "POST").alias("no_post"), \
             apply_cond(col('method') == "HEAD").alias("no_head"), \
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "client_ip", "no_get", "no_post", "no_head")

    log_agg_per_device.show(truncate = False)

    log_agg_per_device.orderBy(col("row_id").desc()).show(truncate=False)

    log_agg_per_device.write.mode("overwrite").format('csv').option("header", True).save(
        "C:\\project_log_files_internal\\src\\internal_files\\log_agg_per_device_file")

    # SnowflakeHelper().save_df_to_snowflake(log_agg_per_device, env.sf_log_agg_per_device_table)
    sfOptions = {
        "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfAccount": "tm57257",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "KAVERI_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "KAVERI_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    log_agg_per_device.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                             "{}".format(
                                                                                 r"kaveri_log_agg_per_device")).mode(
        "overwrite").options(header=True).save()


    # save per device in s3
    #
    # log_agg_per_device.write.mode("overwrite").format('csv').option("header", True).save(
    #     "s3://databrickskaveri/final_layer/curated/log_agg_per_device")

    # LOG_PER_DEVICE HIVE TABLE
    #
    log_agg_per_device.write.mode("overwrite").saveAsTable("log_agg_per_device")
    per_device_hive = spark.sql("select * from log_agg_per_device")
    per_device_hive.show(truncate = False)

    """## #log_agg_across_device"""
    log_agg_across_device = per_device_hive.groupBy("day_hour") \
        .agg(count(col("client_ip")).alias("no_of_clients"), \
             sum(col('no_get')).alias("no_get"), \
             sum(col('no_post')).alias("no_post"), \
             sum(col('no_head')).alias("no_head"), \
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "no_of_clients", "no_get", "no_post", "no_head")

    log_agg_across_device.show(truncate = False)

    log_agg_across_device.write.mode("overwrite").format('csv').option("header", True)\
        .save("C:\\project_log_files_internal\\src\\internal_files\\log_agg_across_device_file")

    # SnowflakeHelper().save_df_to_snowflake(log_agg_across_device, env.sf_log_agg_across_device_table)

    # Save across data in s3

    # log_agg_across_device.write.mode("overwrite").format('csv').option("header", True).save(
    #     "s3://databrickskaveri/final_layer/curated/log_agg_across_device_data")

   # LOG_ACROSS_DEVICE HIVE TABLE

    log_agg_across_device.write.mode("overwrite").saveAsTable("log_agg_across_device")
    across_device_hive = spark.sql("select * from log_agg_across_device")
    across_device_hive.show()

    cleansed_hive =  spark.sql("select count(*) from curate_log_details").show()
    across_device_hive = spark.sql("select count (*) from log_agg_across_device").show()
    per_device_hive = spark.sql("select count(*) from log_agg_per_device").show()

    sfOptions = {
        "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfAccount": "tm57257",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "KAVERI_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "KAVERI_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    log_agg_across_device.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                             "{}".format(
                                                                                 r"kaveri_log_agg_across_device")).mode(
        "overwrite").options(header=True).save()


if __name__ == '__main__':
    log_curated_layer()
