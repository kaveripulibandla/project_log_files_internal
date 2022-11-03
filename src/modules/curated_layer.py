import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace


def log_curated_layer():

    spark = SparkSession.builder.appName("Demo-Project2").config("spark.master",
                                                                 "local").enableHiveSupport().getOrCreate()
    spark

    """# #Log_Details(raw_layer)"""

    # Read CSV File and Write to Table
    df = spark.read.option("delimiter"," ").csv("C:\\Users\\kaverip\\Downloads\\299999.text")
    # df = spark.read.option("delimiter", " ").csv(
    #     "s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")
    df.show(truncate=False)

    # Giving col names to each columns

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

    df_col.write.mode("overwrite").saveAsTable("raw_log_details")
    df_log = spark.sql("select * from raw_log_details")
    df_log.show(truncate = False)

    """## ##Log_details(Cleansed_layer)"""

    # Removing the [] in datetime column by using regex replace func
    df_clean = df_log.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))
    df_clean.show(truncate = False)

    df_date = df_clean.withColumn("datetime", to_timestamp("datetime", "dd/MMM/yyyy:HH:mm:ss")).withColumn('datetime',
                                                                                                           date_format(
                                                                                                               col("datetime"),
                                                                                                               "MM/dd/yyyy HH:mm:ss"))
    df_date.show(truncate = False)
    df_date.printSchema()

    # df_date1 = df_date.select(col("*"),date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss").alias("datetime_format"))

    # Applying the condition to the above df

    cleaned_df = df_date.withColumn("referer_present(YorN)",
                                    when(col("referrer") == "-", "N") \
                                    .otherwise("Y"))

    cleaned_df.show(truncate = False)

    cleansed_data = cleaned_df.drop("referrer")
    cleansed_data.show(truncate=False)

    cleansed_data.write.mode("overwrite").saveAsTable("cleanse_log_details")
    cleansed_hive = spark.sql("select * from cleanse_log_details")
    cleansed_hive.show(truncate = False)

    """## ##Log_details(Curated_layer)"""

    """## #i)Remove any special characters in the request column(% ,- ? =)"""

    remove_spec = cleansed_hive.select('request', regexp_replace('request', '%|,|-|\?=', ''))

    remove_spec.show(truncate=False)

    curated_data = cleansed_hive.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    curated_data.show(truncate=False)

    """## #iv) Replace null with NA"""

    curated_data.na.fill("Nan").show(truncate=False)

    # convert the column size bytes in to kb
    final_curated = curated_data.withColumn("size", round(col("size") / 1024, 2))
    final_curated.show(truncate = False)

    """## #Replace part of get with put in request column"""

    # final_curated = curated_data1.withColumn('method', regexp_replace('method', 'GET', 'PUT'))
    # final_curated.show(truncate=False)

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

    def spliting_date(val):
        return val.split(":")[0]

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

    # Save across data in s3

    # log_agg_across_device.write.mode("overwrite").format('csv').option("header", True).save(
    #     "s3://databrickskaveri/final_layer/curated/log_agg_across_device_data")

    # LOG_ACROSS_DEVICE HIVE TABLE
    #
    log_agg_across_device.write.mode("overwrite").saveAsTable("log_agg_across_device")
    across_device_hive = spark.sql("select * from log_agg_across_device")
    across_device_hive.show()

    across_device_hive = spark.sql("select count (*) from log_agg_across_device").show()
    per_device_hive = spark.sql("select count(*) from log_agg_per_device").show()

if __name__ == '__main__':
    log_curated_layer()