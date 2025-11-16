import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1763259211818 = glueContext.create_dynamic_frame.from_catalog(database="db-youtube-data-cleaned", table_name="cleaned_youtube_json_data", transformation_ctx="AWSGlueDataCatalog_node1763259211818")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1763259281643 = glueContext.create_dynamic_frame.from_catalog(database="db-youtube-data-cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1763259281643")

# Script generated for node Join
Join_node1763259302008 = Join.apply(frame1=AWSGlueDataCatalog_node1763259281643, frame2=AWSGlueDataCatalog_node1763259211818, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1763259302008")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1763259302008, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1763259196266", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1763259457213 = glueContext.getSink(path="s3://de-youtube-data-analysis-bv", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1763259457213")
AmazonS3_node1763259457213.setCatalogInfo(catalogDatabase="db-youtube-data-analysis",catalogTableName="final_analytics")
AmazonS3_node1763259457213.setFormat("glueparquet", compression="snappy")
AmazonS3_node1763259457213.writeFrame(Join_node1763259302008)
job.commit()