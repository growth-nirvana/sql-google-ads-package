-- campaign_stats
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_stats' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_campaign_stats' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

{% if vars.models.switchover_campaign_stats.active == false %}
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
  _gn_id STRING,
  customer_id INT64,
  date DATE,
  _gn_synced TIMESTAMP,
  active_view_impressions INT64,
  active_view_measurability FLOAT64,
  active_view_measurable_cost_micros INT64,
  active_view_measurable_impressions INT64,
  active_view_viewability FLOAT64,
  ad_network_type STRING,
  base_campaign STRING,
  clicks INT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  cost_micros INT64,
  device STRING,
  id INT64,
  impressions INT64,
  interaction_event_types STRING,
  interactions INT64,
  view_through_conversions INT64
);

  -- Step 1: Create temp table for latest batch
  CREATE TEMP TABLE latest_batch AS
  SELECT 
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(campaign__id AS STRING),
      CAST(DATE(segments__date) AS STRING),
      SAFE_CAST(customer__id AS STRING),
      segments__device,
      segments__adNetworkType
    ]))) AS _gn_id,
    CAST(customer__id AS INT64) AS customer_id,
    DATE(segments__date) AS date,
    _time_extracted as _gn_synced,
    SAFE_CAST(metrics__activeViewImpressions AS INT64) AS active_view_impressions,
    SAFE_CAST(metrics__activeViewMeasurability AS FLOAT64) AS active_view_measurability,
    SAFE_CAST(metrics__activeViewMeasurableCostMicros AS INT64) AS active_view_measurable_cost_micros,
    SAFE_CAST(metrics__activeViewMeasurableImpressions AS INT64) AS active_view_measurable_impressions,
    SAFE_CAST(metrics__activeViewViewability AS FLOAT64) AS active_view_viewability,
    segments__adNetworkType AS ad_network_type,
    campaign__baseCampaign AS base_campaign,
    SAFE_CAST(metrics__clicks AS INT64) AS clicks,
    SAFE_CAST(metrics__conversions AS FLOAT64) AS conversions,
    SAFE_CAST(metrics__conversionsValue AS FLOAT64) AS conversions_value,
    SAFE_CAST(metrics__costMicros AS INT64) AS cost_micros,
    segments__device AS device,
    CAST(campaign__id AS INT64) AS id,
    SAFE_CAST(metrics__impressions AS INT64) AS impressions,
    metrics__interactionEventTypes AS interaction_event_types,
    SAFE_CAST(metrics__interactions AS INT64) AS interactions,
    SAFE_CAST(metrics__viewThroughConversions AS INT64) AS view_through_conversions
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
      _gn_id,
      customer_id,
      date,
      _gn_synced,
      active_view_impressions,
      active_view_measurability,
      active_view_measurable_cost_micros,
      active_view_measurable_impressions,
      active_view_viewability,
      ad_network_type,
      base_campaign,
      clicks,
      conversions,
      conversions_value,
      cost_micros,
      device,
      id,
      impressions,
      interaction_event_types,
      interactions,
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
      ad_network_type,
      base_campaign,
      clicks,
      conversions,
      conversions_value,
      cost_micros,
      device,
      id,
      impressions,
      interaction_event_types,
      interactions,
      view_through_conversions
    FROM latest_batch;

  COMMIT TRANSACTION;
{% if drop_source_table %}
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
{% endif %}
END IF;

{% endif %} 