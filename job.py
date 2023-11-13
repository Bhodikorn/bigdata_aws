import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from recipe_transforms import *
from awsglue.dynamicframe import DynamicFrame


# Generated recipe steps for nba-clean-recipe
# Recipe version 2.0
def applyRecipe_node1699695449872(inputFrame, glueContext, transformation_ctx):
    frame = inputFrame.toDF()
    gc = glueContext
    df1 = DataCleaning.RemoveCombined.apply(
        data_frame=frame,
        glue_context=gc,
        transformation_ctx="nba-clean-recipe-df1",
        source_column="Player",
        remove_special_chars=True,
        remove_custom_chars=False,
        remove_numbers=False,
        remove_letters=False,
        remove_custom_value=False,
        remove_all_whitespace=False,
        remove_all_quotes=False,
        remove_all_punctuation=False,
        collapse_consecutive_whitespace=False,
        remove_leading_trailing_whitespace=False,
        remove_leading_trailing_quotes=False,
        remove_leading_trailing_punctuation=False,
    )
    return DynamicFrame.fromDF(df1, gc, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1699695432234 = glueContext.create_dynamic_frame.from_catalog(
    database="nba",
    table_name="curated_data",
    transformation_ctx="AWSGlueDataCatalog_node1699695432234",
)

# Script generated for node Data Preparation Recipe
# Adding configuration for certain Data Preparation recipe steps to run properly
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Recipe name: nba-clean-recipe
# Recipe version: 2.0
DataPreparationRecipe_node1699695449872 = applyRecipe_node1699695449872(
    inputFrame=AWSGlueDataCatalog_node1699695432234,
    glueContext=glueContext,
    transformation_ctx="DataPreparationRecipe_node1699695449872",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1699696136415 = glueContext.write_dynamic_frame.from_catalog(
    frame=DataPreparationRecipe_node1699695449872,
    database="nba-clear",
    table_name="newew",
    transformation_ctx="AWSGlueDataCatalog_node1699696136415",
)

job.commit()
