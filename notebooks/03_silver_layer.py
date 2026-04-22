#This is an orchestrator notebook to execute sql queries for silver transformations

with open("/Workspace/Repos/.../sql/silver/trips_silver.sql", "r") as f:
    query = f.read()

spark.sql(query)
