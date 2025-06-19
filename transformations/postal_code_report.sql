-- postal_code_report
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'postal_code_report' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_postal_code_report' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

{% if vars.models.switchover_postal_code_report.active == false %}
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
  ad_group_id INT64,
  campaign_id INT64,
  geographic_view_resource_name STRING,
  country_criterion_id STRING,
  location_type STRING,
  clicks FLOAT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  cost_micros FLOAT64,
  impressions FLOAT64,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  geo_target_city STRING,
  geo_target_postal_code STRING,
  geo_target_state STRING,
  date DATE,
  run_id INT64,
  tenant STRING,
  _time_extracted TIMESTAMP,
  _time_loaded TIMESTAMP,
  _gn_id STRING,
  _gn_synced TIMESTAMP
);

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
    customer_id,
    ad_group_id,
    campaign_id,
    geographic_view_resource_name,
    country_criterion_id,
    location_type,
    clicks,
    conversions,
    conversions_value,
    cost_micros,
    impressions,
    all_conversions,
    all_conversions_value,
    geo_target_city,
    geo_target_postal_code,
    geo_target_state,
    date,
    run_id,
    tenant,
    _time_extracted,
    _time_loaded,
    _gn_id,
    _gn_synced
  )
  SELECT 
    CAST(customer__id AS INT64),
    CAST(adGroup__id AS INT64),
    CAST(campaign__id AS INT64),
    geographicView__resourceName,
    geographicView__countryCriterionId,
    geographicView__locationType,
    metrics__clicks,
    metrics__conversions,
    metrics__conversionsValue,
    metrics__costMicros,
    metrics__impressions,
    metrics__allConversions,
    metrics__allConversionsValue,
    segments__geoTargetCity,
    segments__geoTargetPostalCode,
    segments__geoTargetState,
    DATE(segments__date),
    run_id,
    tenant,
    _time_extracted,
    _time_loaded,
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(adGroup__id AS STRING), ''),
      COALESCE(CAST(campaign__id AS STRING), ''),
      COALESCE(geographicView__countryCriterionId, ''),
      COALESCE(geographicView__locationType, ''),
      COALESCE(segments__geoTargetPostalCode, ''),
      COALESCE(segments__date, '')
    ))),
    CURRENT_TIMESTAMP()
  FROM latest_batch;

COMMIT TRANSACTION;

{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}

END IF;

{% endif %} 