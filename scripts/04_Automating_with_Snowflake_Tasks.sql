-- Create Incremental Task

CREATE OR REPLACE TASK refresh_sales_fact
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
INSERT INTO sales_reconciliation_fact
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
    AND DATE(s.datetime_local) = f.date
WHERE NOT EXISTS (
    SELECT 1
    FROM sales_reconciliation_fact fact
    WHERE fact.transaction_id = s.transaction_id
      AND fact.channel = s.channel
);

-- Activate Task
ALTER TASK refresh_sales_fact RESUME;

-- How It Works Now
--	1.	Snowpipe loads new files into staging continuously.
--	2.	Task runs daily at 2 AM UTC:
--	•	Checks staging for any transaction_id + channel not already in sales_reconciliation_fact.
--	•	Inserts only those rows.
--	3.	Old rows remain untouched, so the table just grows with new transactions.
