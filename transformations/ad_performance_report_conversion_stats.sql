-- ad_performance_report_conversion_stats
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_performance_report_conversion_stats' %}
{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_ad_performance_report_conversion_stats' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

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
  _gn_id STRING,
  customer_id INT64,
  date DATE,
  _gn_synced TIMESTAMP,
  all_conversions_value FLOAT64,
  conversions_value FLOAT64,
  conversions FLOAT64,
  ad_id INT64,
  conversion_action_name STRING,
  campaign_id INT64,
  ad_group_id INT64,
  device STRING,
  all_conversions FLOAT64,
  run_id INT64
);

-- Only proceed if the source table exists
IF table_exists THEN

-- Step 1: Create temp table for latest batch
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

-- Step 3: Delete + Insert safely
BEGIN TRANSACTION;

  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (SELECT DISTINCT customer_id FROM latest_batch)
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (SELECT DISTINCT customer_id FROM latest_batch);
  END IF;

  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    _gn_id,
    customer_id,
    date,
    _gn_synced,
    all_conversions_value,
    conversions_value,
    conversions,
    ad_id,
    conversion_action_name,
    campaign_id,
    ad_group_id,
    device,
    all_conversions,
    run_id
  )
  SELECT
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(adGroupAd__ad__id AS STRING), ''),
      COALESCE(segments__date, ''),
      COALESCE(segments__conversionActionName, '')
    ))) as _gn_id,
    CAST(customer__id AS INT64) as customer_id,
    DATE(segments__date) as date,
    _time_loaded as _gn_synced,
    metrics__allConversionsValue as all_conversions_value,
    metrics__conversionsValue as conversions_value,
    metrics__conversions as conversions,
    CAST(adGroupAd__ad__id AS INT64) as ad_id,
    segments__conversionActionName as conversion_action_name,
    CAST(campaign__id AS INT64) as campaign_id,
    CAST(adGroup__id AS INT64) as ad_group_id,
    segments__device as device,
    metrics__allConversions as all_conversions,
    run_id
  FROM latest_batch;


COMMIT TRANSACTION;
{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}

END IF;