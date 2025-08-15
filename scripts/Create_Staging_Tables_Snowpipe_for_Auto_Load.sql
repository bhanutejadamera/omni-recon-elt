-- Create Staging Tables

-- POS Sales
CREATE OR REPLACE TABLE stage_pos_sales (
    transaction_id STRING,
    channel STRING,
    store_id STRING,
    product_id STRING,
    currency STRING,
    amount_local FLOAT,
    refund_flag BOOLEAN,
    quantity INT,
    datetime_local TIMESTAMP
);

-- Ecommerce Sales
CREATE OR REPLACE TABLE stage_ecommerce_sales (
    transaction_id STRING,
    channel STRING,
    store_id STRING,
    product_id STRING,
    currency STRING,
    amount_local FLOAT,
    refund_flag BOOLEAN,
    quantity INT,
    datetime_local TIMESTAMP
);

-- Third-Party Sales
CREATE OR REPLACE TABLE stage_thirdparty_sales (
    transaction_id STRING,
    channel STRING,
    store_id STRING,
    product_id STRING,
    currency STRING,
    amount_local FLOAT,
    refund_flag BOOLEAN,
    quantity INT,
    datetime_local TIMESTAMP
);

-- FX Rates
CREATE OR REPLACE TABLE stage_fx_rates (
    date DATE,
    currency STRING,
    exchange_rate FLOAT
);


-- Create Snowpipes

-- POS Pipe
CREATE OR REPLACE PIPE pos_pipe
AS COPY INTO stage_pos_sales
FROM @omni_stage/pos/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- Ecommerce Pipe
CREATE OR REPLACE PIPE ecommerce_pipe
AS COPY INTO stage_ecommerce_sales
FROM @omni_stage/ecommerce/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- Third-Party Pipe
CREATE OR REPLACE PIPE thirdparty_pipe
AS COPY INTO stage_thirdparty_sales
FROM @omni_stage/thirdparty/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- FX Rates Pipe
CREATE OR REPLACE PIPE fx_rates_pipe
AS COPY INTO stage_fx_rates
FROM @omni_stage/reference/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');


-- Trigger Initial Load

ALTER PIPE pos_pipe REFRESH;
ALTER PIPE ecommerce_pipe REFRESH;
ALTER PIPE thirdparty_pipe REFRESH;
ALTER PIPE fx_rates_pipe REFRESH;

-- Test Loaded Data

SELECT * FROM stage_pos_sales LIMIT 5;
SELECT * FROM stage_ecommerce_sales LIMIT 5;
SELECT * FROM stage_thirdparty_sales LIMIT 5;
SELECT * FROM stage_fx_rates LIMIT 5;