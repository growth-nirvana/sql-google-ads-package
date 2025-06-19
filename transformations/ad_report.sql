-- ad_report
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_report' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_ad_report' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

{% if vars.models.switchover_ad_report.active == false %}
select 1
{% else %}
DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

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
  date DATE,
  campaign_id INT64,
  campaign_name STRING,
  campaign_status STRING,
  ad_group_id INT64,
  ad_group_name STRING,
  ad_group_status STRING,
  ad_id INT64,
  ad_added_by_google_ads BOOL,
  ad_device_preference STRING,
  ad_display_url STRING,
  ad_final_mobile_urls STRING,
  ad_final_urls STRING,
  ad_tracking_url_template STRING,
  ad_url_custom_parameters STRING,
  ad_type STRING,
  expanded_text_ad_description STRING,
  expanded_text_ad_description_2 STRING,
  expanded_text_ad_headline_part_1 STRING,
  expanded_text_ad_headline_part_2 STRING,
  expanded_text_ad_headline_part_3 STRING,
  expanded_text_ad_path_1 STRING,
  expanded_text_ad_path_2 STRING,
  legacy_responsive_display_ad_call_to_action_text STRING,
  legacy_responsive_display_ad_description STRING,
  text_ad_description_1 STRING,
  ad_text_ad_description_2 STRING,
  text_ad_headline STRING,
  call_ad_description_1 STRING,
  call_ad_description_2 STRING,
  labels STRING,
  policy_summary_approval_status STRING,
  status STRING,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  clicks INT64,
  cost_micros INT64,
  impressions INT64,
  view_through_conversions INT64,
  _gn_id STRING,
  _gn_synced TIMESTAMP,
  run_id INT64
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
    date,
    campaign_id,
    campaign_name,
    campaign_status,
    ad_group_id,
    ad_group_name,
    ad_group_status,
    ad_id,
    ad_added_by_google_ads,
    ad_device_preference,
    ad_display_url,
    ad_final_mobile_urls,
    ad_final_urls,
    ad_tracking_url_template,
    ad_url_custom_parameters,
    ad_type,
    expanded_text_ad_description,
    expanded_text_ad_description_2,
    expanded_text_ad_headline_part_1,
    expanded_text_ad_headline_part_2,
    expanded_text_ad_headline_part_3,
    expanded_text_ad_path_1,
    expanded_text_ad_path_2,
    legacy_responsive_display_ad_call_to_action_text,
    legacy_responsive_display_ad_description,
    text_ad_description_1,
    ad_text_ad_description_2,
    text_ad_headline,
    call_ad_description_1,
    call_ad_description_2,
    labels,
    policy_summary_approval_status,
    status,
    all_conversions,
    all_conversions_value,
    conversions,
    conversions_value,
    clicks,
    cost_micros,
    impressions,
    view_through_conversions,
    _gn_id,
    _gn_synced,
    run_id
  )
  SELECT 
    CAST(customer__id AS INT64),
    DATE(segments__date),
    CAST(campaign__id AS INT64),
    campaign__name,
    campaign__status,
    CAST(adGroup__id AS INT64),
    adGroup__name,
    adGroup__status,
    CAST(adGroupAd__ad__id AS INT64),
    adGroupAd__ad__addedByGoogleAds,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    adGroupAd__status,
    SAFE_CAST(metrics__allConversions AS FLOAT64),
    SAFE_CAST(metrics__allConversionsValue AS FLOAT64),
    SAFE_CAST(metrics__conversions AS FLOAT64),
    SAFE_CAST(metrics__conversionsValue AS FLOAT64),
    SAFE_CAST(metrics__clicks AS INT64),
    SAFE_CAST(metrics__costMicros AS INT64),
    SAFE_CAST(metrics__impressions AS INT64),
    SAFE_CAST(metrics__viewThroughConversions AS INT64),
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(adGroupAd__ad__id AS STRING), ''),
      COALESCE(CAST(adGroup__id AS STRING), ''),
      COALESCE(CAST(campaign__id AS STRING), ''),
      COALESCE(segments__date, ''),
      COALESCE(campaign__name, ''),
      COALESCE(adGroup__name, ''),
      COALESCE(adGroupAd__ad__type, '')
    ))) AS _gn_id,
    CURRENT_TIMESTAMP() AS _gn_synced,
    run_id
  FROM latest_batch;

COMMIT TRANSACTION;
{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}

END IF;

{% endif %} 