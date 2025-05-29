-- campaign_history
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_campaign' %}

{% if vars.models.switchover_campaign_history.active == false %}
select 1
{% else %}
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
  _gn_start TIMESTAMP,
  id INT64,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  updated_at TIMESTAMP,
  customer_id INT64,
  base_campaign_id INT64,
  ad_serving_optimization_status STRING,
  advertising_channel_subtype STRING,
  advertising_channel_type STRING,
  experiment_type STRING,
  end_date STRING,
  final_url_suffix STRING,
  frequency_caps STRING,
  name STRING,
  optimization_score FLOAT64,
  payment_mode STRING,
  serving_status STRING,
  start_date STRING,
  status STRING,
  tracking_url_template STRING,
  vanity_pharma_display_url_mode STRING,
  vanity_pharma_text STRING,
  video_brand_safety_suitability STRING,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch with deduplication
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT *
  FROM `{{source_dataset}}.{{source_table_id}}`
  QUALIFY RANK() OVER (
    PARTITION BY campaign__id
    ORDER BY _time_extracted DESC
  ) = 1
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  CAST(campaign__id AS INT64) AS id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  TIMESTAMP '9999-01-01' AS updated_at,
  CAST(customer__id AS INT64) AS customer_id,
  CAST(NULL AS INT64) AS base_campaign_id,
  CAST(NULL AS STRING) AS ad_serving_optimization_status,
  CAST(NULL AS STRING) AS advertising_channel_subtype,
  campaign__advertisingChannelType AS advertising_channel_type,
  CAST(NULL AS STRING) AS experiment_type,
  CAST(NULL AS STRING) AS end_date,
  CAST(NULL AS STRING) AS final_url_suffix,
  CAST(NULL AS STRING) AS frequency_caps,
  campaign__name AS name,
  SAFE_CAST(campaign__optimizationScore AS FLOAT64) AS optimization_score,
  CAST(NULL AS STRING) AS payment_mode,
  campaign__servingStatus AS serving_status,
  CAST(NULL AS STRING) AS start_date,
  campaign__status AS status,
  CAST(NULL AS STRING) AS tracking_url_template,
  CAST(NULL AS STRING) AS vanity_pharma_display_url_mode,
  CAST(NULL AS STRING) AS vanity_pharma_text,
  CAST(NULL AS STRING) AS video_brand_safety_suitability,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(customer__id AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(campaign__advertisingChannelType, ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    campaign__name,
    SAFE_CAST(campaign__optimizationScore AS STRING),
    COALESCE(CAST(NULL AS STRING), ''),
    campaign__servingStatus,
    COALESCE(CAST(NULL AS STRING), ''),
    campaign__status,
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), '')
  ))) AS _gn_id
FROM base;

-- Step 2: Handle SCD Type 2 changes
BEGIN TRANSACTION;

  -- Close existing active records that have changed
  UPDATE `{{target_dataset}}.{{target_table_id}}` target
  SET 
    _gn_active = FALSE,
    _gn_end = CURRENT_TIMESTAMP()
  WHERE target._gn_active = TRUE
    AND target.id IN (SELECT id FROM latest_batch)
    AND EXISTS (
      SELECT 1
      FROM latest_batch source
      WHERE source.id = target.id
        AND TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(source.customer_id AS STRING), ''),
          COALESCE(CAST(source.base_campaign_id AS STRING), ''),
          COALESCE(source.ad_serving_optimization_status, ''),
          COALESCE(source.advertising_channel_subtype, ''),
          COALESCE(source.advertising_channel_type, ''),
          COALESCE(source.experiment_type, ''),
          COALESCE(source.end_date, ''),
          COALESCE(source.final_url_suffix, ''),
          COALESCE(source.frequency_caps, ''),
          COALESCE(source.name, ''),
          COALESCE(CAST(source.optimization_score AS STRING), ''),
          COALESCE(source.payment_mode, ''),
          COALESCE(source.serving_status, ''),
          COALESCE(source.start_date, ''),
          COALESCE(source.status, ''),
          COALESCE(source.tracking_url_template, ''),
          COALESCE(source.vanity_pharma_display_url_mode, ''),
          COALESCE(source.vanity_pharma_text, ''),
          COALESCE(source.video_brand_safety_suitability, '')
        ))) != TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(target.customer_id AS STRING), ''),
          COALESCE(CAST(target.base_campaign_id AS STRING), ''),
          COALESCE(target.ad_serving_optimization_status, ''),
          COALESCE(target.advertising_channel_subtype, ''),
          COALESCE(target.advertising_channel_type, ''),
          COALESCE(target.experiment_type, ''),
          COALESCE(target.end_date, ''),
          COALESCE(target.final_url_suffix, ''),
          COALESCE(target.frequency_caps, ''),
          COALESCE(target.name, ''),
          COALESCE(CAST(target.optimization_score AS STRING), ''),
          COALESCE(target.payment_mode, ''),
          COALESCE(target.serving_status, ''),
          COALESCE(target.start_date, ''),
          COALESCE(target.status, ''),
          COALESCE(target.tracking_url_template, ''),
          COALESCE(target.vanity_pharma_display_url_mode, ''),
          COALESCE(target.vanity_pharma_text, ''),
          COALESCE(target.video_brand_safety_suitability, '')
        )))
    );

  -- Insert new records
  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    _gn_start,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    updated_at,
    customer_id,
    base_campaign_id,
    ad_serving_optimization_status,
    advertising_channel_subtype,
    advertising_channel_type,
    experiment_type,
    end_date,
    final_url_suffix,
    frequency_caps,
    name,
    optimization_score,
    payment_mode,
    serving_status,
    start_date,
    status,
    tracking_url_template,
    vanity_pharma_display_url_mode,
    vanity_pharma_text,
    video_brand_safety_suitability,
    _gn_id
  )
  SELECT 
    _gn_start,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    updated_at,
    customer_id,
    base_campaign_id,
    ad_serving_optimization_status,
    advertising_channel_subtype,
    advertising_channel_type,
    experiment_type,
    end_date,
    final_url_suffix,
    frequency_caps,
    name,
    optimization_score,
    payment_mode,
    serving_status,
    start_date,
    status,
    tracking_url_template,
    vanity_pharma_display_url_mode,
    vanity_pharma_text,
    video_brand_safety_suitability,
    _gn_id
  FROM latest_batch source
  WHERE NOT EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}` target
    WHERE target.id = source.id
      AND target._gn_active = TRUE
  );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF;

{% endif %}