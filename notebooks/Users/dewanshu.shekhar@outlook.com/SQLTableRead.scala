// Databricks notebook source
val secret = dbutils.secrets.get(scope = "sql-credentials", key = "Welcome@123")

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "advanalytics.database.windows.net",
  "databaseName"   -> "day03sql",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "admin123",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)
collection.createOrReplaceTempView("customers")
collection.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

    
val config = Config(Map(
  "url"            -> "advanalytics.database.windows.net",
  "databaseName"   -> "day03sql",
  "dbTable"        -> "dbo.Locations",
  "user"           -> "admin123",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection1 = spark.read.sqlDB(config)

collection1.createOrReplaceTempView("locations")
collection1.printSchema


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM locations

// COMMAND ----------

val dlsSecret = dbutils.secrets.get(scope = "training-scope", key = "adbclientaccesskey1")
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "bdca5d18-5709-4c9c-b40d-bb236de77d3a",
  "fs.azure.account.oauth2.client.secret" -> dlsSecret,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@mstranninggen2.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata2",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC WHERE L.State IN ( "AP", "TN", "KA" )

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls /mnt/dlsdata

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("factsales")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP VIEW ProcessedResults
// MAGIC AS
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )

// COMMAND ----------

 val results = spark.sql("SELECT * FROM ProcessedResults")

results.printSchema
results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")

// COMMAND ----------

val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)

// COMMAND ----------

// DBTITLE 1,DB and Polybase Configuration to bulk updates
val blobStorage = "mstrainingstorage.blob.core.windows.net"
	val blobContainer = "tempdata"
	val blobAccessKey =  "7FKvoK22Xc78JCIoaqvA5zjSkFW4uqTWhxf9vKt2cDTyOKyL9fPzMQiSN4ZAnwAKzIwB5in50N+2dyZZB5NypQ=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------


val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

//SQL Data Warehouse related settings
	val dwDatabase = "mstranning"
	val dwServer = "mstranning.database.windows.net"
	val dwUser = "adminuser"
	val dwPass = "Passw0rd"
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass


// COMMAND ----------

results.write
    .format("com.databricks.spark.sqldw")
    .option("url", sqlDwUrlSmall) 
    .option("dbtable", "ProcessedResults")
    .option("forward_spark_azure_storage_credentials","True")
    .option("tempdir", tempDir)
    .mode("overwrite")
    .save()