# Databricks notebook source
database_host = "demo-database9.database.windows.net"
database_port = "1433"
database_name = "database connection "
user = "sqladmin"
password = "Ranapratap@9"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name};user={user};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connectionProperties = {
  "user" : user,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

# COMMAND ----------

df =spark.read.format("jdbc").option("url",url).option("dbtable",f"SalesLT.Address").option("properties","connectionProperties").load()

# COMMAND ----------

df.createOrReplaceTempView('new_tab')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists formula1_dev.bronze.Address(
# MAGIC   
# MAGIC AddressID int,
# MAGIC AddressLine1 string,
# MAGIC AddressLine2 string,
# MAGIC City string,
# MAGIC StateProvince string,
# MAGIC CountryRegion string,
# MAGIC PostalCode string,
# MAGIC rowguid string,
# MAGIC audit_load_ts timestamp
# MAGIC
# MAGIC )
# MAGIC using Delta
# MAGIC location "abfss://bronze@databrickckscourseextdl.dfs.core.windows.net/addres"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into formula1_dev.bronze.Address as a
# MAGIC using new_tab as b
# MAGIC on a.AddressID = b.AddressID
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------


