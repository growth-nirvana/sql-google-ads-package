-- ad_stats
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_stats' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_ad_stats' %}

{% if vars.models.switchover_ad_stats.active == false %}
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
  _gn_id STRING,
  customer_id INT64,
  date DATE,
  _gn_synced TIMESTAMP,
  active_view_impressions INT64,
  active_view_measurability FLOAT64,
  active_view_measurable_cost_micros INT64,
  active_view_measurable_impressions INT64,
  active_view_viewability FLOAT64,
  ad_group_base_ad_group STRING,
  ad_group_id INT64,
  ad_id INT64,
  ad_network_type STRING,
  campaign_base_campaign STRING,
  campaign_id INT64,
  clicks INT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  cost_micros INT64,
  cost_per_conversion FLOAT64,
  device STRING,
  impressions INT64,
  interaction_event_types STRING,
  interactions INT64,
  video_views INT64,
  view_through_conversions INT64
);


-- Step 1: Create temp table for latest batch using run_id
CREATE TEMP TABLE latest_batch AS
SELECT
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(customer__id AS STRING), ''),
    COALESCE(CAST(adGroup__id AS STRING), ''),
    COALESCE(CAST(adGroupAd__ad__id AS STRING), ''),
    COALESCE(CAST(campaign__id AS STRING), ''),
    COALESCE(segments__date, ''),
    COALESCE(segments__adNetworkType, ''),
    COALESCE(segments__device, '')
  ))) AS _gn_id,
  CAST(customer__id AS INT64) AS customer_id,
  DATE(segments__date) AS date,
  CURRENT_TIMESTAMP() AS _gn_synced,
  CAST(metrics__activeViewImpressions AS INT64) AS active_view_impressions,
  CAST(metrics__activeViewMeasurability AS FLOAT64) AS active_view_measurability,
  CAST(metrics__activeViewMeasurableCostMicros AS INT64) AS active_view_measurable_cost_micros,
  CAST(metrics__activeViewMeasurableImpressions AS INT64) AS active_view_measurable_impressions,
  CAST(metrics__activeViewViewability AS FLOAT64) AS active_view_viewability,
  CAST(NULL AS STRING) AS ad_group_base_ad_group,
  CAST(adGroup__id AS INT64) AS ad_group_id,
  CAST(adGroupAd__ad__id AS INT64) AS ad_id,
  segments__adNetworkType AS ad_network_type,
  CAST(NULL AS STRING) AS campaign_base_campaign,
  CAST(campaign__id AS INT64) AS campaign_id,
  CAST(metrics__clicks AS INT64) AS clicks,
  CAST(metrics__conversions AS FLOAT64) AS conversions,
  CAST(metrics__conversionsValue AS FLOAT64) AS conversions_value,
  CAST(metrics__costMicros AS INT64) AS cost_micros,
  CASE 
    WHEN CAST(metrics__conversions AS FLOAT64) > 0 
    THEN CAST(metrics__costMicros AS FLOAT64) / CAST(metrics__conversions AS FLOAT64)
    ELSE 0 
  END AS cost_per_conversion,
  segments__device AS device,
  CAST(metrics__impressions AS INT64) AS impressions,
  metrics__interactionEventTypes AS interaction_event_types,
  CAST(metrics__interactions AS INT64) AS interactions,
  CAST(NULL AS INT64) AS video_views,
  CAST(metrics__viewThroughConversions AS INT64) AS view_through_conversions
FROM `{{source_dataset}}.{{source_table_id}}`
WHERE run_id = (
  SELECT MAX(run_id)
  FROM `{{source_dataset}}.{{source_table_id}}`
);

-- Step 2: Assign min/max dates using SET + scalar subqueries
SET min_date = (
  SELECT MIN(DATE(date)) FROM latest_batch
);

SET max_date = (
  SELECT MAX(DATE(date)) FROM latest_batch
);

-- Step 3: Conditional delete and insert
BEGIN TRANSACTION;

  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (
        SELECT DISTINCT CAST(customer_id AS INT64)
        FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE date BETWEEN min_date AND max_date
      AND customer_id IN (
        SELECT DISTINCT CAST(customer_id AS INT64)
        FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    _gn_id,
    customer_id,
    date,
    _gn_synced,
    active_view_impressions,
    active_view_measurability,
    active_view_measurable_cost_micros,
    active_view_measurable_impressions,
    active_view_viewability,
    ad_group_base_ad_group,
    ad_group_id,
    ad_id,
    ad_network_type,
    campaign_base_campaign,
    campaign_id,
    clicks,
    conversions,
    conversions_value,
    cost_micros,
    cost_per_conversion,
    device,
    impressions,
    interaction_event_types,
    interactions,
    video_views,
    view_through_conversions
  )
  SELECT 
    _gn_id,
    customer_id,
    date,
    _gn_synced,
    active_view_impressions,
    active_view_measurability,
    active_view_measurable_cost_micros,
    active_view_measurable_impressions,
    active_view_viewability,
    ad_group_base_ad_group,
    ad_group_id,
    ad_id,
    ad_network_type,
    campaign_base_campaign,
    campaign_id,
    clicks,
    conversions,
    conversions_value,
    cost_micros,
    cost_per_conversion,
    device,
    impressions,
    interaction_event_types,
    interactions,
    video_views,
    view_through_conversions
  FROM latest_batch;


COMMIT TRANSACTION;
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF;

{% endif %} 