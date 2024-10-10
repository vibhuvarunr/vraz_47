CREATE TABLE `p_supplychain.fact_wes_stock` (

DISTRIBUTION_CENTER_SK   INT64   NOT NULL OPTIONS(description="Site SK from DIM_SITE"),
DISTRIBUTION_CENTER_ID   STRING   NOT NULL OPTIONS(description="Distribution center identification number"),
STOCK_SEQUENCE_NUM   INT64   NOT NULL OPTIONS(description="Sequence number generated in COFE for each DC"),
ITEM_SK   INT64 OPTIONS(description="Links To Dim_Item_History Using Item_Id"),
ITEM_ID   STRING   NOT NULL OPTIONS(description="SKU in the stock record"),
STOCK_STATUS   STRING   NOT NULL OPTIONS(description="Possible status of stock record: • Active: inventory in Active location | • Reserve: inventory in Reserve location | • On Way: inventory in transit | • Problem: inventory with exception"),
PARENT_NAME   STRING OPTIONS(description="Stock location parent"),
PARENT_TYPE   STRING OPTIONS(description="PL for Pick Location | RC for Receiving Carton | TT for Tote"),
STOCK_QTY   INT64 OPTIONS(description="Quantity in stock"),
ORIGINAL_QTY   INT64 OPTIONS(description="Stock originally sent by WM for this stock record"),
PICK_QTY   INT64 OPTIONS(description="Quantity picked"),
ALLOCATION_QTY   INT64 OPTIONS(description="Not used at UB RDC"),
LAST_PICK_DATE_SK   INT64 OPTIONS(description="SK from DIM_CALENDAR"),
LAST_PICK_TIME_SK   INT64 OPTIONS(description="SK from DIM_TIME"),
LAST_PICK_DATETIME   DATETIME OPTIONS(description="Timestamp of last pick for this stock record"),
FLAGS   INT64 OPTIONS(description=""),
LOCK_CODE   STRING OPTIONS(description="WM sent to COFE to set/clear COFE problem stock location"),
ADVANCED_SHIPPING_NOTICE   STRING OPTIONS(description="Not used at UB RDC"),
PURCHASE_ORDER   STRING OPTIONS(description="Not used at UB RDC"),
MERCHANDISE_GROUP   STRING OPTIONS(description="Merchandise group."),
MERCHANDISE_TYPE   STRING OPTIONS(description="Merchandise type."),
CASE_WEIGHT   FLOAT64 OPTIONS(description="Not used at UB RDC"),
CASE_HEIGHT   FLOAT64 OPTIONS(description="Not used at UB RDC"),
CASE_WIDTH   FLOAT64 OPTIONS(description="Not used at UB RDC"),
CASE_LENGTH   FLOAT64 OPTIONS(description="Not used at UB RDC"),
EXPIRATION_DATE_SK   INT64 OPTIONS(description="SK from DIM_CALENDAR"),
EXPIRATION_TIME_SK   INT64 OPTIONS(description="SK from DIM_TIME"),
EXPIRATION_DATETIME   DATETIME OPTIONS(description="Not used at UB RDC"),
UPDATED_DATE_SK   INT64 OPTIONS(description="SK from DIM_CALENDAR"),
UPDATED_TIME_SK   INT64 OPTIONS(description="SK from DIM_TIME"),
UPDATED_DATETIME   DATETIME OPTIONS(description="Timestamp of last activity for the stock"),
RECORD_TYPE1_HASHVALUE   BYTES   NOT NULL OPTIONS(description="Hash Key Based On Column Values"),
BATCH_ID   INT64   NOT NULL OPTIONS(description="Etl Audit Field"),
SOURCE_SYSTEM_NAME   STRING OPTIONS(description="Etl Audit Field"),
SOURCE_CREATED_DATE   DATETIME   NOT NULL OPTIONS(description="Etl Audit Field"),
SOURCE_UPDATED_DATE   DATETIME OPTIONS(description="Etl Audit Field"),
SOURCE_CREATED_BY   STRING OPTIONS(description="Etl Audit Field"),
SOURCE_UPDATED_BY   STRING OPTIONS(description="Etl Audit Field"),
PZ_CREATED_DATE   DATETIME   NOT NULL OPTIONS(description="Etl Audit Field"),
PZ_UPDATED_DATE   DATETIME   NOT NULL OPTIONS(description="Etl Audit Field"),
PZ_CREATED_BY   STRING   NOT NULL OPTIONS(description="Etl Audit Field"),
PZ_UPDATED_BY   STRING   NOT NULL OPTIONS(description="Etl Audit Field"),
)

PARTITION BY date(SOURCE_CREATED_DATE)
OPTIONS( partition_expiration_days=2922.0,require_partition_filter = True,expiration_timestamp = null);
