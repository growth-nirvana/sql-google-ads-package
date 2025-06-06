-- keyword_report_custom_conversions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'keyword_report_custom_conversions' %}
{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_keyword_report_custom_conversions' %}
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

-- Only proceed if the source table exists
IF table_exists THEN

-- Create target table if not exists
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  _gn_id STRING,
  customer_id INT64,
  date DATE,
  _gn_synced TIMESTAMP,
  all_conversions_value FLOAT64,
  conversions_value FLOAT64,
  conversions FLOAT64,
  ad_group_criterion_criterion_id INT64,
  keyword_text STRING,
  conversion_action_name STRING,
  keyword_match_type STRING,
  ad_group_id INT64,
  all_conversions FLOAT64
);

-- Step 1: Create temp table for latest batch using run_id
CREATE TEMP TABLE latest_batch AS
SELECT
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(customer__id AS STRING), ''),
    COALESCE(CAST(adGroup__id AS STRING), ''),
    COALESCE(CAST(adGroupCriterion__criterionId AS STRING), ''),
    COALESCE(adGroupCriterion__keyword__text, ''),
    COALESCE(adGroupCriterion__keyword__matchType, ''),
    COALESCE(CAST(campaign__id AS STRING), ''),
    COALESCE(segments__conversionActionName, ''),
    COALESCE(segments__date, '')
  ))) AS _gn_id,
  CAST(customer__id AS INT64) AS customer_id,
  DATE(segments__date) AS date,
  CURRENT_TIMESTAMP() AS _gn_synced,
  CAST(metrics__allConversionsValue AS FLOAT64) AS all_conversions_value,
  CAST(metrics__conversionsValue AS FLOAT64) AS conversions_value,
  CAST(metrics__conversions AS FLOAT64) AS conversions,
  CAST(adGroupCriterion__criterionId AS INT64) AS ad_group_criterion_criterion_id,
  adGroupCriterion__keyword__text AS keyword_text,
  segments__conversionActionName AS conversion_action_name,
  adGroupCriterion__keyword__matchType AS keyword_match_type,
  CAST(adGroup__id AS INT64) AS ad_group_id,
  CAST(metrics__allConversions AS FLOAT64) AS all_conversions
FROM `{{source_dataset}}.{{source_table_id}}`
WHERE run_id = (
  SELECT MAX(run_id)
  FROM `{{source_dataset}}.{{source_table_id}}`
);

-- Step 2: Capture date range
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
    date,
    _gn_synced,
    all_conversions_value,
    conversions_value,
    conversions,
    ad_group_criterion_criterion_id,
    keyword_text,
    conversion_action_name,
    keyword_match_type,
    ad_group_id,
    all_conversions
  )
  SELECT
    _gn_id,
    customer_id,
    date,
    _gn_synced,
    all_conversions_value,
    conversions_value,
    conversions,
    ad_group_criterion_criterion_id,
    keyword_text,
    conversion_action_name,
    keyword_match_type,
    ad_group_id,
    all_conversions
  FROM latest_batch;

COMMIT TRANSACTION;
{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}

END IF;

