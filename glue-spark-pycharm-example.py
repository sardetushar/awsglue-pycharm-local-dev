from pyspark.sql import SparkSession

import sys
sys.path[0] = 'D:\\projects\\glue_pfa_demo\\com'

from awsglue.context import GlueContext

spark = SparkSession \
    .builder \
    .appName("Glue-PyCharm-Example") \
    .config("spark.jars", "AWSGlueETLPython-1.0.0-jar-with-dependencies.jar") \
    .config("spark.local.dir","D:\\tmp\\spark_glue") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)

inputGDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://my-bucket-name/glue_ex.csv"]}, format = "csv")
outputGDF = glueContext.write_dynamic_frame.from_options(frame = inputGDF, connection_type = "s3", connection_options = {"path": "s3://my-bucket-name/glue_new_ex"}, format = "csv")
