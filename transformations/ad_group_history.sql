-- ad_group_history
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_group_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_adgroups' %}

{% if vars.models.switchover_ad_group_history.active == false %}
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
  campaign_id INT64,
  base_ad_group_id INT64,
  ad_rotation_mode STRING,
  campaign_name STRING,
  display_custom_bid_dimension STRING,
  explorer_auto_optimizer_setting_opt_in BOOL,
  final_url_suffix STRING,
  name STRING,
  status STRING,
  target_restrictions STRING,
  tracking_url_template STRING,
  type STRING,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch with deduplication
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT *
  FROM `{{source_dataset}}.{{source_table_id}}`
  QUALIFY RANK() OVER (
    PARTITION BY adGroup__id
    ORDER BY _time_extracted DESC
  ) = 1
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  CAST(adGroup__id AS INT64) AS id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  TIMESTAMP '9999-01-01' AS updated_at,
  CAST(campaign__id AS INT64) AS campaign_id,
  CAST(NULL AS INT64) AS base_ad_group_id,
  CAST(NULL AS STRING) AS ad_rotation_mode,
  adGroup__campaign AS campaign_name,
  CAST(NULL AS STRING) AS display_custom_bid_dimension,
  CAST(NULL AS BOOL) AS explorer_auto_optimizer_setting_opt_in,
  CAST(NULL AS STRING) AS final_url_suffix,
  adGroup__name AS name,
  adGroup__status AS status,
  CAST(NULL AS STRING) AS target_restrictions,
  CAST(NULL AS STRING) AS tracking_url_template,
  adGroup__type AS type,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(campaign__id AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(adGroup__campaign, ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS BOOL), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    adGroup__name,
    adGroup__status,
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    adGroup__type
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
          COALESCE(CAST(source.campaign_id AS STRING), ''),
          COALESCE(CAST(source.base_ad_group_id AS STRING), ''),
          COALESCE(source.ad_rotation_mode, ''),
          COALESCE(source.campaign_name, ''),
          COALESCE(source.display_custom_bid_dimension, ''),
          COALESCE(CAST(source.explorer_auto_optimizer_setting_opt_in AS STRING), ''),
          COALESCE(source.final_url_suffix, ''),
          COALESCE(source.name, ''),
          COALESCE(source.status, ''),
          COALESCE(source.target_restrictions, ''),
          COALESCE(source.tracking_url_template, ''),
          COALESCE(source.type, '')
        ))) != TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(target.campaign_id AS STRING), ''),
          COALESCE(CAST(target.base_ad_group_id AS STRING), ''),
          COALESCE(target.ad_rotation_mode, ''),
          COALESCE(target.campaign_name, ''),
          COALESCE(target.display_custom_bid_dimension, ''),
          COALESCE(CAST(target.explorer_auto_optimizer_setting_opt_in AS STRING), ''),
          COALESCE(target.final_url_suffix, ''),
          COALESCE(target.name, ''),
          COALESCE(target.status, ''),
          COALESCE(target.target_restrictions, ''),
          COALESCE(target.tracking_url_template, ''),
          COALESCE(target.type, '')
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
    campaign_id,
    base_ad_group_id,
    ad_rotation_mode,
    campaign_name,
    display_custom_bid_dimension,
    explorer_auto_optimizer_setting_opt_in,
    final_url_suffix,
    name,
    status,
    target_restrictions,
    tracking_url_template,
    type,
    _gn_id
  )
  SELECT 
    _gn_start,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    updated_at,
    campaign_id,
    base_ad_group_id,
    ad_rotation_mode,
    campaign_name,
    display_custom_bid_dimension,
    explorer_auto_optimizer_setting_opt_in,
    final_url_suffix,
    name,
    status,
    target_restrictions,
    tracking_url_template,
    type,
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