import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1703346591217 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_Landing_node1703346591217",
)

# Script generated for node Customer Curated
CustomerCurated_node1703346536958 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="CustomerCurated_node1703346536958",
)

# Script generated for node Join
Join_node1703346793560 = Join.apply(
    frame1=Accelerometer_Landing_node1703346591217,
    frame2=CustomerCurated_node1703346536958,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703346793560",
)

# Script generated for node Drop Fields
DropFields_node1703346930892 = DropFields.apply(
    frame=Join_node1703346793560,
    paths=["timestamp", "x", "user", "y", "z"],
    transformation_ctx="DropFields_node1703346930892",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703346972118 = DynamicFrame.fromDF(
    DropFields_node1703346930892.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1703346972118",
)

# Script generated for node customer_curated
customer_curated_node1703346990587 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1703346972118,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hashem-stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1703346990587",
)

job.commit()
