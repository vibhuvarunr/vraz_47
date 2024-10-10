CREATE OR REPLACE PROCEDURE `raw_zone.sp_fact_wes_stock_history`()

OPTIONS (strict_mode=false)
BEGIN
--########################################################################
--##Description    : BIC_AZSCCOFA062_hist to fact_wes_stock
--##Type           : TRUNCATE and LOAD
--##Frequency      : History load one time
--##Author         : Vibhu Varun Ravulapudi
--##Version        : 1.0
--##Version history:
--
--##version		Date		Author	    		            Changes
--##1.0     	19-09-2023  Vibhu Varun Ravulapudi      	Initial version

--##########################################################################

-- Declaring variables 
DECLARE A_PZ_CREATED_BY STRING;
DECLARE A_PZ_UPDATED_BY STRING;
DECLARE A_BATCH_ID INT64;

-- Assigning values to variables
SET A_PZ_CREATED_BY = 'sa-app-composer@__PROJECT_ID__.iam.gserviceaccount.com';
SET A_PZ_UPDATED_BY = 'sa-app-composer@__PROJECT_ID__.iam.gserviceaccount.com';
SET A_BATCH_ID = -1;

TRUNCATE TABLE  `p_supplychain.fact_wes_stock`; --TRUNCATE & LOAD

INSERT INTO  `p_supplychain.fact_wes_stock`
(
DISTRIBUTION_CENTER_SK,
DISTRIBUTION_CENTER_ID,
STOCK_SEQUENCE_NUM,
ITEM_SK,
ITEM_ID,
STOCK_STATUS,
PARENT_NAME,
PARENT_TYPE,
STOCK_QTY,
ORIGINAL_QTY,
PICK_QTY,
ALLOCATION_QTY,
LAST_PICK_DATE_SK,
LAST_PICK_TIME_SK,
LAST_PICK_DATETIME,
FLAGS,
LOCK_CODE,
ADVANCED_SHIPPING_NOTICE,
PURCHASE_ORDER,
MERCHANDISE_GROUP,
MERCHANDISE_TYPE,
CASE_WEIGHT,
CASE_HEIGHT,
CASE_WIDTH,
CASE_LENGTH,
EXPIRATION_DATE_SK,
EXPIRATION_TIME_SK,
EXPIRATION_DATETIME,
UPDATED_DATE_SK,
UPDATED_TIME_SK,
UPDATED_DATETIME,
RECORD_TYPE1_HASHVALUE,
BATCH_ID,
SOURCE_SYSTEM_NAME,
SOURCE_CREATED_DATE,
SOURCE_UPDATED_DATE,
SOURCE_CREATED_BY,
SOURCE_UPDATED_BY,
PZ_CREATED_DATE,
PZ_UPDATED_DATE,
PZ_CREATED_BY,
PZ_UPDATED_BY
)

select

DISTRIBUTION_CENTER_SK,
DISTRIBUTION_CENTER_ID,
STOCK_SEQUENCE_NUM,
ITEM_SK,
ITEM_ID,
STOCK_STATUS,
PARENT_NAME,
PARENT_TYPE,
STOCK_QTY,
ORIGINAL_QTY,
PICK_QTY,
ALLOCATION_QTY,
LAST_PICK_DATE_SK,
LAST_PICK_TIME_SK,
LAST_PICK_DATETIME,
FLAGS,
LOCK_CODE,
ADVANCED_SHIPPING_NOTICE,
PURCHASE_ORDER,
MERCHANDISE_GROUP,
MERCHANDISE_TYPE,
CASE_WEIGHT,
CASE_HEIGHT,
CASE_WIDTH,
CASE_LENGTH,
EXPIRATION_DATE_SK,
EXPIRATION_TIME_SK,
EXPIRATION_DATETIME,
UPDATED_DATE_SK,
UPDATED_TIME_SK,
UPDATED_DATETIME,

SHA256(concat(
IFNULL(CAST(DISTRIBUTION_CENTER_SK AS STRING),'~|*'),
IFNULL(CAST(ITEM_SK AS STRING),'~|*'),
IFNULL(CAST(PARENT_NAME AS STRING),'~|*'),
IFNULL(CAST(PARENT_TYPE AS STRING),'~|*'),
IFNULL(CAST(STOCK_QTY AS STRING),'~|*'),
IFNULL(CAST(ORIGINAL_QTY AS STRING),'~|*'),
IFNULL(CAST(PICK_QTY AS STRING),'~|*'),
IFNULL(CAST(ALLOCATION_QTY AS STRING),'~|*'),
IFNULL(CAST(LAST_PICK_DATE_SK AS STRING),'~|*'),
IFNULL(CAST(LAST_PICK_TIME_SK AS STRING),'~|*'),
IFNULL(CAST(LAST_PICK_DATETIME AS STRING),'~|*'),
IFNULL(CAST(FLAGS AS STRING),'~|*'),
IFNULL(CAST(LOCK_CODE AS STRING),'~|*'),
IFNULL(CAST(ADVANCED_SHIPPING_NOTICE AS STRING),'~|*'),
IFNULL(CAST(PURCHASE_ORDER AS STRING),'~|*'),
IFNULL(CAST(MERCHANDISE_GROUP AS STRING),'~|*'),
IFNULL(CAST(MERCHANDISE_TYPE AS STRING),'~|*'),
IFNULL(CAST(CASE_WEIGHT AS STRING),'~|*'),
IFNULL(CAST(CASE_HEIGHT AS STRING),'~|*'),
IFNULL(CAST(CASE_WIDTH AS STRING),'~|*'),
IFNULL(CAST(CASE_LENGTH AS STRING),'~|*'),
IFNULL(CAST(EXPIRATION_DATE_SK AS STRING),'~|*'),
IFNULL(CAST(EXPIRATION_TIME_SK AS STRING),'~|*'),
IFNULL(CAST(EXPIRATION_DATETIME AS STRING),'~|*'),
IFNULL(CAST(UPDATED_DATE_SK AS STRING),'~|*'),
IFNULL(CAST(UPDATED_TIME_SK AS STRING),'~|*'),
IFNULL(CAST(UPDATED_DATETIME AS STRING),'~|*')
 )) AS RECORD_TYPE1_HASHVALUE,

BATCH_ID,
SOURCE_SYSTEM_NAME,
SOURCE_CREATED_DATE,
SOURCE_UPDATED_DATE,
SOURCE_CREATED_BY,
SOURCE_UPDATED_BY,
PZ_CREATED_DATE,
PZ_UPDATED_DATE,
PZ_CREATED_BY,
PZ_UPDATED_BY

from
(

SELECT

     ifnull(DIM_SITE.SITE_SK,-1) as DISTRIBUTION_CENTER_SK, -- Not Nullable
     ifnull(nullif(lpad(raw.ZDC_ID,4,'0'),''),'Not Applicable')  as DISTRIBUTION_CENTER_ID, -- Not Nullable  -- PK
	 ifnull(cast(ZIDX as int64),-1) as STOCK_SEQUENCE_NUM, -- Not Nullable  -- PK
	 
     dim_item_history.ITEM_SK as ITEM_SK,
	 
     if(ltrim(ZSKU,'0') = '' OR nullif(ZSKU,'') is null OR ZSKU is null, 'Not Applicable',ZSKU) as ITEM_ID, -- Not Nullable  -- PK
	 if(ltrim(ZSTATUS,'0') = '' OR nullif(ZSTATUS,'') is null OR ZSTATUS is null, 'Not Applicable',ZSTATUS) as STOCK_STATUS, -- Not Nullable  -- PK
	 	 
	 ZPARNA as PARENT_NAME,
	 ZPARTYP as PARENT_TYPE,
	 
	 cast(ZQTY      as int64) as STOCK_QTY,
	 cast(ZORGQTY   as int64) as ORIGINAL_QTY,
	 cast(ZPICKQTY  as int64) as PICK_QTY,
	 cast(ZALLOCQTY as int64) as ALLOCATION_QTY,
	 
     DIM_CALENDAR1.calendar_sk as LAST_PICK_DATE_SK,	 -- is TIMESTAMP --> LASTPICKTIME?
     
	 DIM_TIME1.TIME_SK as LAST_PICK_TIME_SK, -- LASTPICKTIME is it nullable?
	 
	 if(nullif(ltrim(ZLASPITIME,'0'),'') IS NULL or ZLASPITIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZLASPITIME))) as LAST_PICK_DATETIME,
	 
     cast(zflags as int64) as flags,
	 
	 ZLOCKCODE     as LOCK_CODE,	 
	 ZASN          as ADVANCED_SHIPPING_NOTICE,
     ZPO           as PURCHASE_ORDER,
     ZMERGRP       as MERCHANDISE_GROUP,
     ZMERTYP       as MERCHANDISE_TYPE,
     
	 CAST(ifnull(ZCASEWEIG,0.0) AS FLOAT64) as CASE_WEIGHT,     
     CAST(ifnull(ZCASEHEIG,0.0) AS FLOAT64) as CASE_HEIGHT,     
     CAST(ifnull(ZCASEWID,0.0)  AS FLOAT64) as CASE_WIDTH,      
     CAST(ifnull(ZCASELEN,0.0)  AS FLOAT64) as CASE_LENGTH,     
	 
	 
     DIM_CALENDAR2.calendar_sk as EXPIRATION_DATE_SK,
	 
     DIM_TIME2.TIME_SK         as EXPIRATION_TIME_SK,
	 
	 if(nullif(ltrim(ZEXPDAT,'0'),'') IS NULL or ZEXPDAT is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZEXPDAT))) as EXPIRATION_DATETIME,
	  
	 
	 DIM_CALENDAR3.calendar_sk                as UPDATED_DATE_SK,
	 
     DIM_TIME3.TIME_SK                        as UPDATED_TIME_SK,
	 
     if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))) as UPDATED_DATETIME,

--   Generated Key  as RECORD_TYPE1_HASHVALUE,	  

--   ETL Fields
     A_BATCH_ID           as BATCH_ID,
     'HANA'               as SOURCE_SYSTEM_NAME,
     if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))) as SOURCE_CREATED_DATE,
     if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))) as SOURCE_UPDATED_DATE,
	 cast(null as string) as SOURCE_CREATED_BY,
	 cast(null as string) as SOURCE_UPDATED_BY,
     current_datetime()   as PZ_CREATED_DATE,
     current_datetime()   as PZ_UPDATED_DATE,
     A_PZ_CREATED_BY      AS PZ_CREATED_BY,
     A_PZ_UPDATED_BY      AS PZ_UPDATED_BY
   
	
	 FROM 
	 (
	 select * from (  
        select * from raw_zone.BIC_AZSCCOFA062_hist 
        WHERE 
	    ifnull(safe.PARSE_DATE('%Y%m%d',if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'00010101',ZUPDTIME)),'1111-11-11') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 7 YEAR)
     
     )hist_stat
     inner join
     p_supplychain.Stock_Status sk_status
     ON
     hist_stat.zstatus=sk_status.status
	 QUALIFY row_number() over (partition by 
     ifnull(nullif(lpad(ZDC_ID,4,'0'),''),'Not Applicable'),
     ifnull(cast(ZIDX as int64),-1) ,
     if(ltrim(ZSKU,'0') = '' OR nullif(ZSKU,'') is null OR ZSKU is null, 'Not Applicable',ZSKU),
     if(ltrim(ZSTATUS,'0') = '' OR nullif(ZSTATUS,'') is null OR ZSTATUS is null, 'Not Applicable',ZSTATUS)
     order by sk_status.status_code desc, ifnull(safe.PARSE_DATE('%Y%m%d',if(nullif(ltrim(zupdtime,'0'),'') IS NULL or zupdtime is null,'00010101',zupdtime)),'1111-11-11') desc)=1



	 --QUALIFY ROW_NUMBER() OVER
	 --(PARTITION BY 
	 --ifnull(nullif(lpad(raw.ZDC_ID,4,'0'),''),'Not Applicable'),
	 --ifnull(cast(ZIDX as int64),-1),
	 --if(ltrim(ZSKU,'0') = '' OR nullif(ZSKU,'') is null OR ZSKU is null, 'Not Applicable',ZSKU),
	 --if(ltrim(ZSTATUS,'0') = '' OR nullif(ZSTATUS,'') is null OR ZSTATUS is null, 'Not Applicable',ZSTATUS)
	 --ORDER BY INGEST_DATE DESC) = 1
	 ) raw
	 
     left join p_masterdata.dim_site DIM_SITE
     on  ifnull(nullif(lpad(raw.ZDC_ID,4,'0'),''),'Not Applicable') = DIM_SITE.SITE_ID and
     if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))) between DIM_SITE.RECORD_EFF_START_DTTM and DIM_SITE.RECORD_EFF_END_DTTM
	 
	 left join p_masterdata.dim_item_history dim_item_history
     on if(ltrim(ZSKU,'0') = '' OR nullif(ZSKU,'') is null OR ZSKU is null, 'Not Applicable',ZSKU) = dim_item_history.ITEM_ID and
	 if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))) between dim_item_history.RECORD_EFF_START_DTTM and dim_item_history.RECORD_EFF_END_DTTM
     
	 left join p_masterdata.dim_calendar DIM_CALENDAR1
	 on ifnull(safe.PARSE_DATE('%Y%m%d',if(nullif(ltrim(ZLASPITIME,'0'),'') IS NULL or ZLASPITIME is null,'00010101',ZLASPITIME)),'1111-11-11') = DIM_CALENDAR1.DAY_DATE
	 
	 LEFT JOIN p_masterdata.dim_time DIM_TIME1
     ON format_time('%I:%M:%S %p',TIME(if(nullif(ltrim(ZLASPITIME,'0'),'') IS NULL or ZLASPITIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZLASPITIME))))) = DIM_TIME1.TIME_12
	 	 
     left join p_masterdata.dim_calendar DIM_CALENDAR2
	 on ifnull(safe.PARSE_DATE('%Y%m%d',if(nullif(ltrim(ZEXPDAT,'0'),'') IS NULL or ZEXPDAT is null,'00010101',ZEXPDAT)),'1111-11-11') = DIM_CALENDAR2.DAY_DATE
	 
	 LEFT JOIN p_masterdata.dim_time DIM_TIME2
     ON format_time('%I:%M:%S %p',TIME(if(nullif(ltrim(ZEXPDAT,'0'),'') IS NULL or ZEXPDAT is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZEXPDAT))))) = DIM_TIME2.TIME_12
	 
     left join p_masterdata.dim_calendar DIM_CALENDAR3
	 on ifnull(safe.PARSE_DATE('%Y%m%d',if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'00010101',ZUPDTIME)),'1111-11-11') = DIM_CALENDAR3.DAY_DATE
	 
	 LEFT JOIN p_masterdata.dim_time DIM_TIME3
     ON format_time('%I:%M:%S %p',TIME(if(nullif(ltrim(ZUPDTIME,'0'),'') IS NULL or ZUPDTIME is null,'0001-01-01T00:00:00',datetime(parse_date('%Y%m%d',ZUPDTIME))))) = DIM_TIME3.TIME_12
	 
);

END;
