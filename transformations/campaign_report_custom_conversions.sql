-- campaign_report_custom_conversions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_report_custom_conversions' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_campaign_report_custom_conversions' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

{% if vars.models.switchover_campaign_report_custom_conversions.active == false %}
select 1
{% else %}
DECLARE min_date DATE;
DECLARE max_date DATE;
DECLARE table_exists BOOL DEFAULT FALSE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  base_campaign STRING,
  conversion_action_name STRING,
  conversions FLOAT64,
  conversions_value FLOAT64,
  customer_id INT64,
  date DATE,
  _gn_id STRING,
  _gn_synced TIMESTAMP,
  id INT64,
  name STRING,
  status STRING,
  run_id INT64
);

-- Only proceed if the source table exists
IF table_exists THEN

-- Step 1: Create temp table for latest batch using run_id
CREATE TEMP TABLE latest_batch AS
SELECT *
FROM `{{source_dataset}}.{{source_table_id}}`
WHERE run_id = (
  SELECT MAX(run_id)
  FROM `{{source_dataset}}.{{source_table_id}}`
);

-- Step 2: Assign min/max dates using SET + scalar subqueries
SET min_date = (
  SELECT MIN(DATE(segments__date)) FROM latest_batch
);

SET max_date = (
  SELECT MAX(DATE(segments__date)) FROM latest_batch
);

-- Step 3: Conditional delete and insert
BEGIN TRANSACTION;

  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (
        SELECT DISTINCT CAST(customer__id AS INT64)
        FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (
        SELECT DISTINCT CAST(customer__id AS INT64)
        FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    all_conversions,
    all_conversions_value,
    base_campaign,
    conversion_action_name,
    conversions,
    conversions_value,
    customer_id,
    date,
    _gn_id,
    _gn_synced,
    id,
    name,
    status,
    run_id
  )
  SELECT 
    CAST(metrics__allConversions AS FLOAT64),
    CAST(metrics__allConversionsValue AS FLOAT64),
    CAST(NULL AS STRING),
    CAST(segments__conversionActionName AS STRING),
    CAST(metrics__conversions AS FLOAT64),
    CAST(metrics__conversionsValue AS FLOAT64),
    CAST(customer__id AS INT64),
    DATE(segments__date),
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(campaign__id AS STRING), ''),
      COALESCE(segments__date, ''),
      COALESCE(segments__conversionActionName, ''),
      COALESCE(campaign__name, ''),
      COALESCE(CAST(campaign__id AS STRING), '')
    ))) AS _gn_id,
    CURRENT_TIMESTAMP(),
    CAST(campaign__id AS INT64),
    campaign__name,
    CAST(NULL AS STRING),
    run_id
  FROM latest_batch;
  

COMMIT TRANSACTION;

{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}

END IF;

{% endif %}
