create or replace storage integration raw_data_ing
TYPE = EXTERNAL_STAGE
ENABLED = TRUE 
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::645154692040:role/sf-storage-intg'
 STORAGE_ALLOWED_LOCATIONS = ('s3://sf-data-intg/');

 SHOW 
 INTEGRATIONS ;

 DESC INTEGRATION RAW_DATA_ING;

  CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1

;

CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = RAW_DATA_ING
  URL = 's3://sf-data-intg/'
  FILE_FORMAT = my_csv_format;

copy into my_dummy from @my_s3_stage
PATTERN='.*jaffle_shop_customers.*[.]csv';
