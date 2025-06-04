from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yfinance as yf

# Option A: Connecting to a specific cluster
# Replace 'your_cluster_id' with the actual ID of your Databricks cluster.
# You can find this in the URL of your cluster details page in Databricks workspace:
# https://<your-workspace-url>/?o=<org-id>#setting/clusters/<cluster-id>/configuration
spark = DatabricksSession.builder.getOrCreate()
# df_tcs = yf.download('TCS.NS').reset_index()
# sdf_tcs = spark.createDataFrame(df_tcs)

# Option B: Connecting to a serverless SQL warehouse
# Replace 'your_warehouse_id' with the actual ID of your serverless SQL warehouse.
# You can find this in the URL of your SQL warehouse details page:
# https://<your-workspace-url>/?o=<org-id>#sql/warehouses/<warehouse-id>
#spark = DatabricksSession.builder.serverless().getOrCreate()

# Example usage (after successful connection)
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df.write.mode('overwrite').saveAsTable('dbacademy.labuser10344139_1749015561.df')
df.show()
#pycharm checkout
##test

# Don't forget to stop the Spark session when done
#spark.stop()
