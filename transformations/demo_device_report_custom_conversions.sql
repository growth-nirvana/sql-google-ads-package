-- demo_device_report_custom_conversions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'demo_device_report_custom_conversions' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_demo_device_custom_conversions' %}

{% if vars.models.demo_device_report_custom_conversions.active == false %}
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

-- Only proceed if the source table exists
IF table_exists THEN

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  customer_id INT64,
  campaign_id INT64,
  date DATE,
  device STRING,
  conversion_action_name STRING,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  view_through_conversions FLOAT64,
  tenant STRING,
  _time_extracted TIMESTAMP,
  _time_loaded TIMESTAMP,
  _gn_id STRING,
  _gn_synced TIMESTAMP
);

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
        SELECT DISTINCT SAFE_CAST(customer__id AS INT64)
        FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (
        SELECT DISTINCT SAFE_CAST(customer__id AS INT64)
        FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    customer_id,
    campaign_id,
    date,
    device,
    conversion_action_name,
    all_conversions,
    all_conversions_value,
    conversions,
    conversions_value,
    view_through_conversions,
    tenant,
    _time_extracted,
    _time_loaded,
    _gn_id,
    _gn_synced
  )
  SELECT 
    SAFE_CAST(customer__id AS INT64),
    SAFE_CAST(campaign__id AS INT64),
    DATE(segments__date),
    segments__device,
    segments__conversionActionName,
    SAFE_CAST(metrics__allConversions AS FLOAT64),
    SAFE_CAST(metrics__allConversionsValue AS FLOAT64),
    SAFE_CAST(metrics__conversions AS FLOAT64),
    SAFE_CAST(metrics__conversionsValue AS FLOAT64),
    SAFE_CAST(metrics__viewThroughConversions AS FLOAT64),
    tenant,
    _time_extracted,
    _time_loaded,
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(campaign__id AS STRING), ''),
      COALESCE(DATE(segments__date), ''),
      COALESCE(segments__device, ''),
      COALESCE(segments__conversionActionName, ''),
      COALESCE(tenant, '')
    ))) AS _gn_id,
    CURRENT_TIMESTAMP() AS _gn_synced
  FROM latest_batch;

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF;

{% endif %} 