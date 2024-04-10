  create or replace file format my_parquet_format
  type = 'parquet';

  CREATE STAGE dbt.my_s3_stage_pg
  STORAGE_INTEGRATION = RAW_DATA_ING
  URL = 's3://sf-data-intg/'
  FILE_FORMAT = my_parquet_format
  match_by_column_name = case_insensitive;



 CREATE TABLE car_data (
    am INT,
    carb INT,
    cyl INT,
    disp FLOAT,
    drat FLOAT,
    gear INT,
    hp INT,
    model VARCHAR(255),
    mpg FLOAT,
    qsec FLOAT,
    vs INT,
    wt FLOAT
);


COPY INTO car_data
FROM  @dbt.my_s3_stage_pg/parquet_file
match_by_column_name = case_insensitive;
