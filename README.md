## Incremental Load and SCD Type 2 Implementation in PySpark

This repository demonstrates a PySpark-based implementation of an incremental data load process and Slowly Changing Dimension (SCD) Type 2 logic using Delta Lake in Databricks. The project focuses on ensuring data quality, applying business transformations, and maintaining historical records efficiently.

#### Features
1. ##### Incremental Data Load:
- Reads new booking and customer data based on a date parameter.
- Performs data quality checks using PyDeequ to validate the datasets.
- Applies business transformations and writes aggregated results to a Delta table.
2. ##### SCD Type 2 Implementation:
- Maintains historical customer records using SCD2 logic.
- Updates the existing records with valid_to dates and appends new records with valid_from and valid_to columns.
3. ##### Data Quality Checks:
- Ensures dataset integrity with checks like:
- Non-null constraints.
- Uniqueness checks for primary keys.
- Non-negative values for specific columns.
4. ##### Business Transformations:
- Calculates total cost after discounts.
- Aggregates data by booking type and customer for key metrics like total sales and quantity.
5. ##### Delta Lake Integration:
- Uses Delta Lake for efficient data storage, schema enforcement, and change tracking.

#### Workflow Overview
1. ##### Data Ingestion
- Reads booking and customer data files from specified file paths using a date parameter (arrival_date).
- Validates data integrity using PyDeequ.

2. ##### Incremental Load for Booking Data
- Adds an ingestion timestamp to the booking data.
- Joins booking data with customer data for enrichment.
- Applies transformations, such as calculating total cost after discounts and filtering invalid records.
- Aggregates data by booking type and customer to calculate total sales and quantity.
- Writes the results to a Delta table (booking_fact).

3. ##### SCD Type 2 Implementation for Customer Data
- Checks if the customer Delta table (customer_dim) exists:
- If it exists, performs an SCD2 merge:
- Updates the valid_to column for existing records.
- Appends new records with updated valid_from and valid_to columns.
- If it doesn’t exist, creates a new Delta table for the customer data.

#### Key Technologies
- PySpark: For ETL, data transformations, and aggregations.
- PyDeequ: For data quality validation.
- Delta Lake: To handle incremental loads and implement SCD2.
- Databricks: For notebook execution and workflow management.

#### How to Use
##### Prerequisites
- Databricks environment set up with a Delta-enabled cluster.
- Data files placed in the appropriate storage paths:
- /Volumes/travel_booking/default/scd2_data/booking-data/
- /Volumes/travel_booking/default/scd2_data/customer-data/

#### Steps to Run
1. Clone this repository to your Databricks workspace.
2. Set the required job parameter:
- arrival_date: Date string in the format YYYY-MM-DD (e.g., "2024-07-25").
3. Execute the notebook:
- The notebook reads, validates, processes, and writes data incrementally.
- It performs SCD2 updates for the customer dimension table.

#### Data Quality Checks
##### Booking Data
- Size Check: Ensures the dataset is not empty.
- Uniqueness: Validates unique booking_id.
- Completeness: Ensures no nulls in critical columns (customer_id, amount, quantity, discount).
- Non-Negative Values: Checks that amount, quantity, and discount are non-negative.

##### Customer Data
- Size Check: Ensures the dataset is not empty.
- Uniqueness: Validates unique customer_id.
- Completeness: Ensures no nulls in key columns (customer_name, customer_address, email).

##### Output
- Booking Fact Table (booking_fact):
  - Aggregated metrics by booking_type and customer_id.
  - Stored as a Delta table for incremental updates.
- Customer Dimension Table (customer_dim):
  - Historical records maintained using SCD Type 2 logic.