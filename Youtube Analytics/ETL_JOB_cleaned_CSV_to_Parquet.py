import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_extract, input_file_name

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


raw_df = spark.read.format("csv") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .option("multiline", True) \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .option("delimiter", ",") \
    .load("s3://de-youtube-data-raw-bv/youtube/raw_statistics/")


clean_df = raw_df \
    .withColumn("views", col("views").cast("bigint")) \
    .withColumn("likes", col("likes").cast("bigint")) \
    .withColumn("dislikes", col("dislikes").cast("bigint")) \
    .withColumn("comment_count", col("comment_count").cast("bigint")) \
    .withColumn("category_id", col("category_id").cast("bigint"))


if "region" not in clean_df.columns:
    clean_df = clean_df.withColumn(
        "region",
        regexp_extract(input_file_name(), r"region=([^/]+)/", 1)
    )


output_path = "s3://de-youtube-data-cleaned-bv/youtube/raw_statistics/"

clean_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("region") \
    .save(output_path)

job.commit()
