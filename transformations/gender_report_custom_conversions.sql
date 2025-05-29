-- gender_report_custom_conversions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'gender_report_custom_conversions' %}
{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_gender_report_custom_conversions' %}

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
  _gn_id STRING,
  customer_id INT64,
  ad_group_id INT64,
  criterion_id INT64,
  gender_type STRING,
  campaign_id INT64,
  conversion_action_name STRING,
  date DATE,
  _gn_synced TIMESTAMP,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  tenant STRING
);

-- Step 1: Create temp table for latest batch using run_id
CREATE TEMP TABLE latest_batch AS
SELECT
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(customer__id AS STRING), ''),
    COALESCE(CAST(adGroup__id AS STRING), ''),
    COALESCE(CAST(adGroupCriterion__criterionId AS STRING), ''),
    COALESCE(adGroupCriterion__gender__type, ''),
    COALESCE(CAST(campaign__id AS STRING), ''),
    COALESCE(segments__conversionActionName, ''),
    COALESCE(DATE(segments__date), ''),
    COALESCE(tenant, '')
  ))) AS _gn_id,
  CAST(customer__id AS INT64) AS customer_id,
  CAST(adGroup__id AS INT64) AS ad_group_id,
  CAST(adGroupCriterion__criterionId AS INT64) AS criterion_id,
  adGroupCriterion__gender__type AS gender_type,
  CAST(campaign__id AS INT64) AS campaign_id,
  segments__conversionActionName AS conversion_action_name,
  DATE(segments__date) AS date,
  CURRENT_TIMESTAMP() AS _gn_synced,
  metrics__allConversions AS all_conversions,
  metrics__allConversionsValue AS all_conversions_value,
  metrics__conversions AS conversions,
  metrics__conversionsValue AS conversions_value,
  tenant
FROM `{{source_dataset}}.{{source_table_id}}`
WHERE run_id = (
  SELECT MAX(run_id)
  FROM `{{source_dataset}}.{{source_table_id}}`
);

-- Step 2: Assign min/max dates using SET + scalar subqueries
SET min_date = (SELECT MIN(date) FROM latest_batch);
SET max_date = (SELECT MAX(date) FROM latest_batch);

-- Step 3: Delete and insert data
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
    ad_group_id,
    criterion_id,
    gender_type,
    campaign_id,
    conversion_action_name,
    date,
    _gn_synced,
    all_conversions,
    all_conversions_value,
    conversions,
    conversions_value,
    tenant
  )
  SELECT
    _gn_id,
    customer_id,
    ad_group_id,
    criterion_id,
    gender_type,
    campaign_id,
    conversion_action_name,
    date,
    _gn_synced,
    all_conversions,
    all_conversions_value,
    conversions,
    conversions_value,
    tenant
  FROM latest_batch;

COMMIT TRANSACTION;
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 