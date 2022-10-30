import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F


def log_cleansed_layer():

	spark = SparkSession.builder.appName("Demo-Project2").config("spark.master","local").enableHiveSupport().getOrCreate()
	spark

	"""# #Log_Details(raw_layer)"""

	# Read CSV File and Write to Table
	# df = spark.read.option("delimiter"," ").csv("C:\\Users\\kaverip\\Downloads\\299999.text")
	df = spark.read.option("delimiter"," ").csv("s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")
	df.show(truncate = False)

	# Giving col names to each columns

	# import pyspark.sql.functions as F
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

	"""## ##Log_details(Cleansed_layer)"""

	# Removing the [] in datetime column by using regex replace func
	df_clean = df_col.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))
	df_clean.show()

	df_date = df_clean.withColumn("datetime",to_timestamp("datetime","dd/MMM/yyyy:HH:mm:ss"))
	df_date.show()
	df_date.printSchema()

	# df_date1 = df_date.select(col("*"),date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss").alias("datetime_format"))
	df_dateformat = df_date.withColumn("datetime", date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss"))
	df_dateformat.show()

	df_dateformat_con = df_dateformat.withColumn("datetime",to_timestamp("datetime","MM-dd-yyyy:HH:mm:ss"))
	df_dateformat_con.show()

	df_dateformat_con.printSchema()

	# Applying the condition to the above df

	cleaned_df = df_date.withColumn("referer_present(YorN)",
	                            when(col("referrer")=="-" ,"N") \
	                            .otherwise("Y"))

	cleaned_df.show()

	cleansed_data = cleaned_df.drop("referrer")
	cleansed_data.show(truncate = False)
	cleansed_data.write.mode("overwrite").saveAsTable("cleansed_data_table")
	cleansed_hive = spark.sql("select * from cleansed_data_table")
	cleansed_hive.show()

	"""## #i)Remove any special characters in the request column(% ,- ? =)"""

	remove_spec = cleansed_data.select('request', regexp_replace('request', '%|,|-|\?=', ''))

	remove_spec.show(truncate = False)

	cleansed_data = cleansed_data.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
	cleansed_data.show(truncate = False)

	"""## #iv) Replace null with NA"""

	cleansed_data.na.fill("Nan").show(truncate = False)

	# convert the column size bytes in to kb
	cleansed_data1 = cleansed_data.withColumn("size",round(col("size")/1024,2))
	cleansed_data1.show()



	"""## #Replace part of get with put in request column"""

	#Replace part of get with put in request column
	from pyspark.sql.functions import regexp_replace

	final_cleansed = cleansed_data1.withColumn('method', regexp_replace('method', 'GET', 'PUT'))
	final_cleansed.show(truncate=False)

	final_cleansed.write.mode("overwrite").format('csv').option("header",True).save("s3://databrickskaveri/final_layer/cleansed/cleansed_data")
	# final_cleansed.write.mode("overwrite").saveAsTable("curated_data_table")
	# curated_hive = spark.sql("select * from curated_data_table")
	# curated_hive.show()

if __name__ == '__main__':
	log_cleansed_layer()