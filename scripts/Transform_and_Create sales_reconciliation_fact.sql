-- Create the Fact Table

CREATE OR REPLACE TABLE sales_reconciliation_fact AS
SELECT
    s.transaction_id,
    s.channel,
    s.store_id,
    s.product_id,
    s.currency,
    s.amount_local,
    ROUND(s.amount_local / f.exchange_rate, 2) AS amount_usd,
    s.refund_flag,
    s.quantity,
    s.datetime_local,
    YEAR(s.datetime_local) AS year,
    MONTH(s.datetime_local) AS month
FROM (
    SELECT * FROM stage_pos_sales
    UNION ALL
    SELECT * FROM stage_ecommerce_sales
    UNION ALL
    SELECT * FROM stage_thirdparty_sales
) s
LEFT JOIN stage_fx_rates f
    ON s.currency = f.currency
    AND DATE(s.datetime_local) = f.date;


-- Test the Unified Table
SELECT * FROM sales_reconciliation_fact LIMIT 10;