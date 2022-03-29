// Databricks notebook source
// MAGIC %md
// MAGIC # XML file ingestion in realtime with Autoloader

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS xml_demo;

// COMMAND ----------

// MAGIC %sql
// MAGIC USE xml_demo;

// COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/autoloader-test/xml/vinci/landing")

// COMMAND ----------

val basePath = "dbfs:/tmp/autoloader-test/xml"
val onlineOrder_path = s"$basePath/vinci"
val landing_folder = s"$onlineOrder_path/landing/vinci*.xml"; // Landing folder for new files

val rawTableLocation = s"$onlineOrder_path/raw"; // Bronze Table Destination
val rawCheckPointLocation = s"$onlineOrder_path/raw/checkpoint"; // Bronze Table Chekpointing dir

val bronzeTableLocation = s"$onlineOrder_path/bronze"; // Bronze Table Destination
val bronzeCheckPointLocation = s"$onlineOrder_path/bronze/checkpoint"; // Bronze Table Chekpointing dir

val silverTableLocation = s"$onlineOrder_path/silver"; // Bronze Table Destination

val raw_table = "onlineOrder_raw" // Bronze Table Name
val bronze_table = "onlineOrder_bronze" // Bronze Table Name
val silver_table = "onlineOrder_silver" // Silver Table Name

// COMMAND ----------

// MAGIC %md
// MAGIC ## Clean up 

// COMMAND ----------

//cleanup
dbutils.fs.rm(rawTableLocation, true)
dbutils.fs.rm(bronzeTableLocation, true)
dbutils.fs.rm(silverTableLocation, true)

dbutils.fs.rm(rawCheckPointLocation, true)
dbutils.fs.rm(bronzeCheckPointLocation, true)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS onlineOrder_raw;
// MAGIC DROP TABLE IF EXISTS onlineOrder_bronze;
// MAGIC DROP TABLE IF EXISTS onlineOrder_silver;
// MAGIC DROP TABLE IF EXISTS onlineOrder_gold;

// COMMAND ----------

// MAGIC %md
// MAGIC #  Realtime XML Files ingestion with Autoloader

// COMMAND ----------

// DBTITLE 1,AutoLoader loads files in real time as they land in Cloud Storage
import spark.implicits._
val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8")) // UDF to convert the binary to String

val fileDF = spark.readStream.format("cloudFiles")
  .option("cloudFiles.useNotifications", "false")
  .option("cloudFiles.format", "binaryFile")
  .load(landing_folder)
  .select(toStrUDF($"content").alias("payload"))

// COMMAND ----------

display(fileDF)

// COMMAND ----------

// DBTITLE 1,Saving the stream of XML FIles into RAW Layer in real time
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val bronzeDF = fileDF
   .withColumn("load_source",input_file_name) 
   .withColumn("load_timestamp", current_timestamp())
   .withColumn("load_date", to_date('load_timestamp))
   .writeStream
   .outputMode("append")
   .trigger(Trigger.ProcessingTime("2 seconds"))
   .option("path", rawTableLocation)
   .option("checkpointLocation", rawCheckPointLocation)
   .option("mergeSchema","true")  //OR set spark.databricks.delta.schema.autoMerge.enabled
   //.trigger(Trigger.Once())       // To run this job periodically instead of streaming   uncomment  this line   
   .toTable(raw_table)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM onlineOrder_raw

// COMMAND ----------

// MAGIC %md
// MAGIC # Bronze - Parse XML to JSON with xmltodict

// COMMAND ----------

// MAGIC %python
// MAGIC pip install xmltodict

// COMMAND ----------

// MAGIC %python
// MAGIC bronzeTableLocation = "dbfs:/tmp/autoloader-test/xml/vinci/bronze"; # Bronze Table Destination
// MAGIC bronzeCheckPointLocation = "dbfs:/tmp/autoloader-test/xml/vinci/bronze/checkpoint"; # Bronze Table Chekpointing dir
// MAGIC bronze_table = "onlineOrder_bronze" #  Bronze Table Name

// COMMAND ----------

// DBTITLE 1,How it works?
// MAGIC %python
// MAGIC import xmltodict, json
// MAGIC 
// MAGIC obj = xmltodict.parse("""
// MAGIC <employees>
// MAGIC 	<employee>
// MAGIC   		<name>Dave</name>
// MAGIC         <role>Sale Assistant</role>
// MAGIC         <age>34</age>
// MAGIC     </employee>
// MAGIC </employees>
// MAGIC """)
// MAGIC print(json.dumps(obj))

// COMMAND ----------

// DBTITLE 1,We can create a python UDF function
// MAGIC %python
// MAGIC from pyspark.sql.functions import udf
// MAGIC from pyspark.sql.types import StringType
// MAGIC 
// MAGIC def xml2JSON(s):
// MAGIC   obj = xmltodict.parse(s)
// MAGIC   print (obj)
// MAGIC   return json.dumps(obj)
// MAGIC 
// MAGIC spark.udf.register("xml2json", xml2JSON)
// MAGIC 
// MAGIC xml2JSON_udf = udf(xml2JSON, StringType())

// COMMAND ----------

// DBTITLE 1,We can use the UDF in SQL
// MAGIC %sql 
// MAGIC SELECT payload AS xml_raw, xml2JSON(payload) AS json_parsed 
// MAGIC FROM onlineOrder_raw

// COMMAND ----------

// DBTITLE 1,Save as Delta Table in Bronze Layer
// MAGIC %python
// MAGIC spark.readStream \
// MAGIC   .table("onlineOrder_raw") \
// MAGIC   .withColumn("payload",xml2JSON_udf("payload").alias("parsed_in_json")) \
// MAGIC   .writeStream \
// MAGIC   .trigger(processingTime='5 seconds') \
// MAGIC   .outputMode("append") \
// MAGIC   .option("path", bronzeTableLocation)  \
// MAGIC   .option("checkpointLocation", bronzeCheckPointLocation)  \
// MAGIC   .option("mergeSchema","true")  \
// MAGIC   .toTable(bronze_table)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from onlineOrder_bronze

// COMMAND ----------

// DBTITLE 1,SQL directly on the semi structured data
// MAGIC %sql
// MAGIC SELECT 
// MAGIC   payload:svcOnlineOrderResponse:response:report:reference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:myInformationModule:orderReference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:companyId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:refId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:vat,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:officialCompanyName,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:nic,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:headEstablishment:address
// MAGIC FROM onlineOrder_bronze

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY onlineOrder_bronze

// COMMAND ----------

// MAGIC %md
// MAGIC # Silver - Curated layer

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS onlineOrder_silver 
// MAGIC USING DELTA TBLPROPERTIES (delta.enableChangeDataCapture = true) 
// MAGIC LOCATION "dbfs:/tmp/autoloader-test/xml/vinci/silver"
// MAGIC AS SELECT 
// MAGIC   payload:svcOnlineOrderResponse:response:report:reference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:myInformationModule:orderReference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:companyId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:refId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:vat,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:officialCompanyName,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:nic,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:headEstablishment:address
// MAGIC FROM onlineOrder_bronze;

// COMMAND ----------

// DBTITLE 1,YEEEE!!!! From XML to JSON and now a Curated tabular table
// MAGIC %sql
// MAGIC SELECT * FROM onlineOrder_silver

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY onlineOrder_silver

// COMMAND ----------

// MAGIC %md
// MAGIC INGEST MORE DATA

// COMMAND ----------

// MAGIC %md
// MAGIC databricks fs cp vinci4.xml dbfs:/tmp/autoloader-test/xml/vinci/landing/vinci4.xml

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO onlineOrder_silver
// MAGIC SELECT 
// MAGIC   payload:svcOnlineOrderResponse:response:report:reference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:myInformationModule:orderReference,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:companyId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:refId,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:vat,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:officialCompanyName,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:nic,
// MAGIC   payload:svcOnlineOrderResponse:response:report:identityModule:headEstablishment:address
// MAGIC FROM onlineOrder_bronze
// MAGIC WHERE payload:svcOnlineOrderResponse:response:report:reference > 222222;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY onlineOrder_silver

// COMMAND ----------

// DBTITLE 1,Let's see all our changes, from delta version 0:
// MAGIC %sql
// MAGIC SELECT * FROM table_changes('onlineOrder_silver', 1) 

// COMMAND ----------

// DBTITLE 1,Capture all Changes
// MAGIC %sql
// MAGIC SELECT _change_type as Type, count(*) as Count
// MAGIC FROM table_changes('onlineOrder_silver', 0) 
// MAGIC GROUP BY _change_type

// COMMAND ----------

// DBTITLE 1,It also works using a range of version or date:
// MAGIC %sql 
// MAGIC SELECT * FROM table_changes('onlineOrder_silver', '2021-09-02T22:28:01')

// COMMAND ----------

// MAGIC %md
// MAGIC # GOLD Change data Capture

// COMMAND ----------

// MAGIC %md ### Delta CDC gives back 4 cdc types in the "__cdc_type" column:
// MAGIC 
// MAGIC | CDC Type             | Description                                                               |
// MAGIC |----------------------|---------------------------------------------------------------------------|
// MAGIC | **update_preimage**  | Content of the row before an update                                       |
// MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
// MAGIC | **delete**           | Content of a row that has been deleted                                    |
// MAGIC | **insert**           | Content of a new row that has been inserted                               |
// MAGIC 
// MAGIC Therefore, 1 update will result in 2 rows in the cdc stream (one row with the previous values, one with the new values)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ###Delta Tables are ACID -  We car  run some DELETE and UPDATE in our table to see the changes:

// COMMAND ----------

// MAGIC %md
// MAGIC Company with ID/Reference 222222 aks for  "GDPR Right to be Forgotten"
// MAGIC Also Company with ID/Reference 3333333 has change the officialCompanyName to a one much more COOL!!!!
// MAGIC With Delta is just a simple ACID operation, no need to read the whole table

// COMMAND ----------

// MAGIC %sql 
// MAGIC DELETE FROM onlineOrder_silver WHERE reference = 222222;
// MAGIC UPDATE onlineOrder_silver SET officialCompanyName='Rafa Arana. Inc' WHERE reference = 3333333;

// COMMAND ----------

// MAGIC %md ###Synchronizing our downstream GOLD table based on the CDC from the Silver Delta Table
// MAGIC Synchronizing our downstream GOLD table based on the CDC from the Silver Delta Table

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT _change_type as Type, count(*) as Count
// MAGIC FROM table_changes('onlineOrder_silver', 0) 
// MAGIC GROUP BY _change_type

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS onlineOrder_gold  (officialCompanyName STRING, vat STRING, address STRING, reference BIGINT NOT NULL) USING delta;

// COMMAND ----------

// MAGIC %python
// MAGIC from delta.tables import *
// MAGIC last_version = str(DeltaTable.forName(spark, "onlineOrder_silver").history().head()["version"])
// MAGIC print("our Delta table last version is {}, let's select the last changes to see our DELETE and UPDATE operations (last 2 versions):".format(last_version))
// MAGIC 
// MAGIC changes = spark.sql("SELECT * FROM table_changes('onlineOrder_silver', {}-1)".format(last_version))
// MAGIC display(changes)

// COMMAND ----------

// DBTITLE 1,Reading the CDC using the Python API by version
// MAGIC %python
// MAGIC cdc_values = spark.read.format("delta") \
// MAGIC                        .option("readChangeData", "true") \
// MAGIC                        .option("startingVersion", int(last_version) -1) \
// MAGIC                        .table("onlineOrder_silver")
// MAGIC cdc_values.createOrReplaceTempView("cdc_values")
// MAGIC display(cdc_values)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- getting the latest change is still needed if the cdc contains multiple time the same id. We can rank over the id and get the most recent __log_version
// MAGIC MERGE INTO onlineOrder_gold target USING
// MAGIC   (select officialCompanyName, vat, address, reference, _change_type from 
// MAGIC     (SELECT *, RANK() OVER (PARTITION BY reference ORDER BY _commit_version DESC) as rank from cdc_values) 
// MAGIC    where rank = 1 and _change_type!='update_preimage'
// MAGIC   ) as source
// MAGIC ON source.reference = target.reference
// MAGIC --   Apply a delete when it is matched and cdc type is delete
// MAGIC   WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
// MAGIC --   Apply an update when matched and cdc type is not delete (thus implies that it is an update)
// MAGIC   WHEN MATCHED AND source._change_type != 'delete' THEN UPDATE SET *
// MAGIC --   Apply an insert when not matched and cdc type is not delete (thus implies that it is an insert)
// MAGIC   WHEN NOT MATCHED AND source._change_type != 'delete' THEN INSERT *

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from onlineOrder_gold;

// COMMAND ----------

// MAGIC %md
// MAGIC ### CDC all Changes to GOLD since last time we run the pipeline

// COMMAND ----------

// MAGIC %sql 
// MAGIC UPDATE onlineOrder_silver SET officialCompanyName='NEW Rafa Arana. Inc' WHERE reference = 3333333;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY onlineOrder_gold

// COMMAND ----------

// DBTITLE 1,Now, let's find out  the last time we changed the Gold table
// MAGIC %python
// MAGIC last_timestamp = str(DeltaTable.forName(spark, "onlineOrder_gold").history().head()["timestamp"])
// MAGIC last_timestamp

// COMMAND ----------

// DBTITLE 1,Capture the changes since that time in Silver, using CDC and  Python API
// MAGIC %python
// MAGIC cdc_values = spark.read.format("delta") \
// MAGIC                        .option("readChangeData", "true") \
// MAGIC                        .option("startingTimestamp", last_timestamp) \
// MAGIC                        .table("onlineOrder_silver")
// MAGIC cdc_values.createOrReplaceTempView("cdc_values")
// MAGIC display(cdc_values)

// COMMAND ----------

// DBTITLE 1,MERGE the cdc to the gold table. Again we need to exclude "update_preimage"
// MAGIC %sql
// MAGIC -- getting the latest change is still needed if the cdc contains multiple time the same id. We can rank over the id and get the most recent __log_version
// MAGIC MERGE INTO onlineOrder_gold target USING
// MAGIC   (select officialCompanyName, vat, address, reference, _change_type from 
// MAGIC     (SELECT *, RANK() OVER (PARTITION BY reference ORDER BY _commit_version DESC) as rank from cdc_values) 
// MAGIC    where rank = 1 and _change_type!='update_preimage'
// MAGIC   ) as source
// MAGIC ON source.reference = target.reference
// MAGIC --   Apply a delete when it is matched and cdc type is delete
// MAGIC   WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
// MAGIC --   Apply an update when matched and cdc type is not delete (thus implies that it is an update)
// MAGIC   WHEN MATCHED AND source._change_type != 'delete' THEN UPDATE SET *
// MAGIC --   Apply an insert when not matched and cdc type is not delete (thus implies that it is an insert)
// MAGIC   WHEN NOT MATCHED AND source._change_type != 'delete' THEN INSERT *

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY onlineOrder_gold

// COMMAND ----------

// MAGIC %sql
// MAGIC select reference, officialCompanyName from onlineOrder_gold where reference=3333333;
