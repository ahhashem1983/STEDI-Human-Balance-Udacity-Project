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

# Script generated for node accelerometer_landing
accelerometer_landing_node1703341783059 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1703341783059",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node1703341821402 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="Customer_Trusted_node1703341821402",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1703341886657 = Join.apply(
    frame1=Customer_Trusted_node1703341821402,
    frame2=accelerometer_landing_node1703341783059,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyJoin_node1703341886657",
)

# Script generated for node Drop Fields
DropFields_node1703342040007 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1703341886657,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1703342040007",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1703343024113 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703342040007,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hashem-stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1703343024113",
)

job.commit()
