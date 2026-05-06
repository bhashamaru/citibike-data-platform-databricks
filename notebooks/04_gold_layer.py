#This is an orchestrator notebook to execute sql queries for silver transformations

with open("/Workspace/Repos/.../sql/silver/gold_tables.sql", "r") as f:
    query = f.read()

spark.sql(query)
