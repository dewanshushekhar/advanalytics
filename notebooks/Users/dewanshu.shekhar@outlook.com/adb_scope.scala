// Databricks notebook source
val secret = dbutils.secrets.get(scope = "sql-credentials", key = "sqlpassword")
print(secret)

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "advanalytics.database.windows.net",
  "databaseName"   -> "day03sql",
  "dbTable"        -> "dbo.customers",
  "user"           -> "admin123",
  "password"       -> "Welcome@123",
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection1 = spark.read.sqlDB(config)

collection1.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from customers

// COMMAND ----------

// DBTITLE 1,DW and Polybase configuration
val blobStorage = "dewanshu03blob.blob.core.windows.net"
	val blobContainer = "tempdata"
	val blobAccessKey =  "fOEnKCyjM4faQm/20Cey5vyOptcjrvtUwlsB2qwbGo5n1TdTxdVyLjqKfQAJhmNr2FACf7x72OltfJWyWsi/rA=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

	val dwDatabase = "advsqldw"
	val dwServer = "advanalytics.database.windows.net"
	val dwUser = "admin123"
	val dwPass = "Welcome@123"
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

	spark.conf.set(
		"spark.sql.parquet.writeLegacyFormat",
		"true")



// COMMAND ----------

results.write
    .format("com.databricks.spark.sqldw")
    .option("url", sqlDwUrlSmall) 
    .option("dbtable", "ProcessedResults")
    .option("forward_spark_azure_storage_credentials","True")
    .option("tempdir", tempDir)
    .mode("overwrite")
    .save()