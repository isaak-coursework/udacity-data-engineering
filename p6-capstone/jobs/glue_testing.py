from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)

df = glueContext.create_dynamic_frame_from_catalog(
    database="athenacurcfn_cur_monthly",
    table_name="monthly"
)

df.printSchema()
