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

# Script generated for node customer_curated
customer_curated_node1703354733777 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1703354733777",
)

# Script generated for node step_trainer
step_trainer_node1703354734656 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_node1703354734656",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1703354843339 = ApplyMapping.apply(
    frame=customer_curated_node1703354733777,
    mappings=[
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("customername", "string", "right_customername", "string"),
        ("email", "string", "right_email", "string"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1703354843339",
)

# Script generated for node Join
Join_node1703354812269 = Join.apply(
    frame1=step_trainer_node1703354734656,
    frame2=RenamedkeysforJoin_node1703354843339,
    keys1=["serialnumber"],
    keys2=["right_serialnumber"],
    transformation_ctx="Join_node1703354812269",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1703354877807 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1703354812269,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hashem-stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1703354877807",
)

job.commit()
