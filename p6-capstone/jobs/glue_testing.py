from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)

df = glueContext.read.csv('s3a://p6-capstone/data/airport-codes_csv.csv')

df.printSchema()
