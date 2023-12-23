import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1703357154018 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1703357154018",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1703357156680 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="step_training_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1703357156680",
)

# Script generated for node Join
Join_node1703357502155 = Join.apply(
    frame1=AWSGlueDataCatalog_node1703357156680,
    frame2=AWSGlueDataCatalog_node1703357154018,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1703357502155",
)

# Script generated for node Amazon S3
AmazonS3_node1703357613711 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1703357502155,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hashem-stedi/step_trainer/ML/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1703357613711",
)

job.commit()
