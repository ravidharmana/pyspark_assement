# Reading data in csv
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("person_ID", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("first", StringType(), True),
                    StructField("last", StringType(), True),
                    StructField("middle", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("fax", StringType(), True),
                    StructField("title", StringType(), True)
])

df = spark.read.options(header='True').schema(schema).csv('C:/Users/RaviDharmana/Desktop/bigdata_input/people.csv')
df.show()
df.printSchema()
df.count()

# write data in delta
df.write.mode("overwrite").format("delta").save('C:/Users/RaviDharmana/Desktop/bigdata_output/peoples')
# Reading data in delta format
df_delta = spark.read.options(header='True').format("delta").load('C:/Users/RaviDharmana/Desktop/bigdata_output/peoples/part-00000-2782f9f5-70aa-42a6-919f-00fa8b03f5bd-c000.snappy.parquet')
df_delta.show()
# count
df_delta.count()