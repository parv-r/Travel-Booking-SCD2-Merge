# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import os

# Get job parameters from Databricks
print(os.environ['SPARK_VERSION'])

# COMMAND ----------

date_str = dbutils.widgets.get("arrival_date")
print(date_str)

# date_str = "2024-07-25"

# Define file paths based on date parameter
booking_data = f"/Volumes/travel_booking/default/scd2_data/booking-data/bookings_{date_str}.csv"
customer_data = f"/Volumes/travel_booking/default/scd2_data/customer-data/customers_{date_str}.csv"
print(booking_data)
print(customer_data)

# Read booking data
# booking_df = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("quote", "\"") \
#     .option("multiLine", "true") \
#     .load(booking_data)
booking_df = spark.read.csv(booking_data, header=True, inferSchema=True, quote='"', multiLine=True)

booking_df.printSchema()
display(booking_data)

# Read customer data for scd2 merge
# customer_df = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("quote", "\"") \
#     .option("multiLine", "true") \
#     .load(customer_data)
customer_df = spark.read.csv(customer_data, header=True, inferSchema=True, quote='"', multiLine=True)

customer_df.printSchema()
display(customer_df)

# COMMAND ----------

# Data Quality Checks on booking data
check_incremental = Check(spark, CheckLevel.Error, "Booking Data Check") \
    .hasSize(lambda x: x > 0) \
    .isUnique("booking_id", hint="Booking ID is not unique throught") \
    .isComplete("customer_id") \
    .isComplete("amount") \
    .isNonNegative("amount") \
    .isNonNegative("quantity") \
    .isNonNegative("discount")

# Data Quality Checks on customer data
# check_scd = Check(spark, CheckLevel.Error, "Customer Data Check") \
#     .hasSize(lambda x: x > 0) \
#     .isUnique("customer_id") \
#     .isComplete("customer_name") \
#     .isComplete("customer_address") \
#     .isComplete("phone_number") \
#     .isComplete("email")

check_scd = Check(spark, CheckLevel.Error, "Customer Data Check") \
    .hasSize(lambda x: x > 0) \
    .isUnique("customer_id") \
    .isComplete("customer_name") \
    .isComplete("customer_address") \
    .isComplete("email")

# Run the verification suite
booking_dq_check = VerificationSuite(spark) \
    .onData(booking_df) \
    .addCheck(check_incremental) \
    .run()

customer_dq_check = VerificationSuite(spark) \
    .onData(customer_df) \
    .addCheck(check_scd) \
    .run()

booking_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, booking_dq_check)
display(booking_dq_check_df)

customer_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, customer_dq_check)
display(customer_dq_check_df)

# Check if verification passed
if booking_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Booking Data")

if customer_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Customer Data")

# COMMAND ----------

# Add ingestion timestamp to booking data
booking_df_incremental = booking_df.withColumn("ingestion_time", current_timestamp())

# Join booking data with customer data
df_joined = booking_df_incremental.join(customer_df, "customer_id")

# Business transformation: calculate total cost after discount and filter
df_transformed = df_joined \
    .withColumn("total_cost", col("amount") - col("discount")) \
    .filter(col("quantity") > 0)

# Group by and aggregate df_transformed
df_transformed_agg = df_transformed \
    .groupBy("booking_type", "customer_id") \
    .agg(
        _sum("total_cost").alias("total_amount_sum"),
        _sum("quantity").alias("total_quantity_sum")
    )

# COMMAND ----------

# Check if the Delta table exists
fact_table_path = "travel_booking.default.booking_fact"
fact_table_exists = spark._jsparkSession.catalog().tableExists(fact_table_path)

# Check if the fact table exists
if fact_table_exists:
    # Read the existing fact table
    df_existing_fact = spark.read.format("delta").table(fact_table_path)
    
    # Combine the aggregated data
    df_combined = df_existing_fact.unionByName(df_transformed_agg, allowMissingColumns=True)
    
    # Perform another group by and aggregation on the combined data
    df_final_agg = df_combined \
        .groupBy("booking_type", "customer_id") \
        .agg(
            _sum("total_amount_sum").alias("total_amount_sum"),
            _sum("total_quantity_sum").alias("total_quantity_sum")
        )
else:
    # If the fact table doesn't exist, use the aggregated transformed data directly
    df_final_agg = df_transformed_agg

display(df_final_agg)

# Write the final aggregated data back to the Delta table
df_final_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(fact_table_path)

# COMMAND ----------

scd_table_path = "travel_booking.default.customer_dim"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

# Check if the customers table exists
if scd_table_exists:
    # Load the existing SCD table
    scd_table = DeltaTable.forName(spark, scd_table_path)
    display(scd_table.toDF())
    
    # Perform SCD2 merge logic
    scd_table.alias("scd") \
        .merge(
            customer_df.alias("updates"),
            "scd.customer_id = updates.customer_id and scd.valid_to = '9999-12-31'"
        ) \
        .whenMatchedUpdate(set={
            "valid_to": "updates.valid_from",
        }) \
        .execute()

    customer_df.write.format("delta").mode("append").saveAsTable(scd_table_path)
else:
    # If the SCD table doesn't exist, write the customer data as a new Delta table
    customer_df.write.format("delta").mode("overwrite").saveAsTable(scd_table_path)
