from pyspark import SparkConf, SparkContext

from pyspark.sql.types import *
import pyspark.sql.functions as F





from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.ml import PipelineModel






def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer1")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("Error")
    stream()






def stream():




    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()




    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("maxFilesPerTrigger", 1)\
        .option("subscribe", "test") \
        .load()


    Schema = StructType().add("tweet", StringType()).add("Target", IntegerType())

    df1 = df.select(F.col("value")\
        .cast("string")\
        .alias("json"))\
        .select(from_json(F\
        .col("json"), Schema)\
        .alias("data"))

    df2 = df1.select("data.*")

    model = PipelineModel.load("pipe1/")

    df3 = model.transform(df2)

    df3 = df3.select("prediction").groupBy("prediction").count()

    df3.writeStream \
        .outputMode("complete")  \
        .format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()


if __name__=="__main__":
    main()
