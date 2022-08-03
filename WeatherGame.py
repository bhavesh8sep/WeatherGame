import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import when
from pyspark.sql.types import StringType, BooleanType, IntegerType, DecimalType
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("WeatherGame").getOrCreate()


# Random data generation for all possible columns
#
superset_df = spark.read.csv("/mnt/c/Users/Bhavesh Patel/Downloads/Input.csv", header = True, inferSchema = True) \
.withColumn('elevation', rand()*115 + 5) \
.withColumn('temperature', rand()*70 - 23) \
.withColumn('pressure', rand()*200 + 900) \
.withColumn('humidity', rand()*100) \
.withColumn('year', rand()*4 + 2018) \
.withColumn('month', rand()*11 + 1) \
.withColumn('date', rand()*27 + 1) \
.withColumn('hour', rand()*24) \
.withColumn('minute', rand()*60) \
.withColumn('second', rand()*60)

subset_df_unconcate = superset_df.select(superset_df.city,superset_df.lat.cast(DecimalType(20,2)).cast(StringType()), \
superset_df.lng.cast(DecimalType(20,2)).cast(StringType()),superset_df.elevation.cast(IntegerType()).cast(StringType()), \
superset_df.temperature.cast(DecimalType(20,1)),superset_df.pressure.cast(DecimalType(20,1)), \
superset_df.humidity.cast(IntegerType()),superset_df.year.cast(IntegerType()), \
lpad(trim(superset_df.month.cast(DecimalType(20,0)).cast(StringType())),2,"0").alias("month"), \
lpad(trim(superset_df.date.cast(DecimalType(20,0)).cast(StringType())),2,"0").alias("date"), \
lpad(trim(superset_df.hour.cast(IntegerType()).cast(StringType())),2,"0").alias("hour"), \
lpad(trim(superset_df.minute.cast(IntegerType()).cast(StringType())),2,"0").alias("minute"), \
lpad(trim(superset_df.second.cast(IntegerType()).cast(StringType())),2,"0").alias("second"),)

final_df = subset_df_unconcate.select(subset_df_unconcate.city, \
concat_ws(",",subset_df_unconcate.lat,subset_df_unconcate.lng,subset_df_unconcate.elevation).alias("position"), \
date_format(concat(F.col("year"),F.lit("-"),F.col("month"),F.lit("-"),F.col("date"),F.lit(" "), \
F.col("hour"),F.lit(":"),F.col("minute"),F.lit(":"),F.col("second")),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias("local_time"), \
when(subset_df_unconcate.temperature < 0, "Snow").when((subset_df_unconcate.temperature > 0) & (subset_df_unconcate.humidity > 80), "Rain").otherwise("Sunny").alias("conditions"), \
when(subset_df_unconcate.temperature < 0, subset_df_unconcate.temperature).otherwise(concat(F.lit("+"),F.col("temperature"))).alias("temperature"),  \
subset_df_unconcate.pressure,subset_df_unconcate.humidity,)

final_df.coalesce(1).write.format("csv").mode("overwrite").option("sep","|").save("/mnt/c/Users/Bhavesh Patel/Downloads/WeatherGame.csv")