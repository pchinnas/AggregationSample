import pyspark
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import functions as f
import sys
#Get the start date and end date to filter based on date range
from_date=sys.argv[1]
to_date=sys.argv[2]
incoming_timestamp=sys.argv[3]
spark = SparkSession.builder.master("local").config("spark.sql.warehouse.dir", "file:///C:/Prabhu/Genesys/warehouse").appName("aggregationSample").getOrCreate()
# define input schema
dataschema = StructType(
    [
        StructField("Metric", StringType(), True),
        StructField("Value", FloatType(), True),
        StructField("Timestamp", StringType(), True),
    ]
)
# read the input file
df = spark.read.load("file:///C:/Prabhu/Genesys/input", schema=dataschema, format="csv", header=True)
df.printSchema()
# convert the timestamp from Eastern time zone to UTC in this we could handle data coming from different time zones
df=df.select(df.Metric,df.Value,f.to_date(f.to_utc_timestamp(df.Timestamp, incoming_timestamp)).alias('utc_time'))
# filter based on the date range
df=df.filter(df.utc_time >= from_date).filter(df.utc_time <= to_date)
# group by Metric and TimeStamp finding the min, max and average of values
df=df.groupBy("Metric","utc_time").agg(f.avg("Value").alias("average_value"),f.min("Value").alias("min_value"),f.max("value").alias("max_value"))
df.show()

