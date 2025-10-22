from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.remote(cluster_id="0813-075425-ixi1v92q").getOrCreate()
spark.sql("SELECT 1").show()
print(spark.version)