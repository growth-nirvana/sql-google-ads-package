-- ad_history
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_ad' %}

{% if vars.models.switchover_ad_history.active == false %}
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
  ad_group_id INT64,
  id INT64,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  action_items STRING,
  ad_strength STRING,
  added_by_google_ads BOOL,
  device_preference STRING,
  display_url STRING,
  final_app_urls STRING,
  final_mobile_urls STRING,
  final_url_suffix STRING,
  final_urls STRING,
  name STRING,
  policy_summary_approval_status STRING,
  policy_summary_review_status STRING,
  status STRING,
  system_managed_resource_source STRING,
  tracking_url_template STRING,
  type STRING,
  updated_at TIMESTAMP,
  url_collections STRING,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch with deduplication
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT *
  FROM `{{source_dataset}}.{{source_table_id}}`
  QUALIFY RANK() OVER (
    PARTITION BY adGroupAd__ad__id, adGroup__id
    ORDER BY _time_extracted DESC
  ) = 1
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  CAST(adGroup__id AS INT64) AS ad_group_id,
  CAST(adGroupAd__ad__id AS INT64) AS id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  CAST(NULL AS STRING) AS action_items,
  CAST(NULL AS STRING) AS ad_strength,
  CAST(NULL AS BOOL) AS added_by_google_ads,
  CAST(NULL AS STRING) AS device_preference,
  CAST(NULL AS STRING) AS display_url,
  CAST(NULL AS STRING) AS final_app_urls,
  CAST(NULL AS STRING) AS final_mobile_urls,
  CAST(NULL AS STRING) AS final_url_suffix,
  CAST(NULL AS STRING) AS final_urls,
  CAST(NULL AS STRING) AS name,
  CAST(NULL AS STRING) AS policy_summary_approval_status,
  CAST(NULL AS STRING) AS policy_summary_review_status,
  adGroupAd__status AS status,
  CAST(NULL AS STRING) AS system_managed_resource_source,
  CAST(NULL AS STRING) AS tracking_url_template,
  adGroupAd__ad__type AS type,
  TIMESTAMP '9999-01-01' AS updated_at,
  CAST(NULL AS STRING) AS url_collections,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(adGroup__id AS STRING), ''),
    COALESCE(CAST(adGroupAd__ad__id AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(adGroupAd__status, ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(adGroupAd__ad__type AS STRING), ''),
    CAST(TIMESTAMP '9999-01-01' AS STRING),
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
    AND EXISTS (
      SELECT 1
      FROM latest_batch
      WHERE latest_batch.id = target.id
        AND latest_batch.ad_group_id = target.ad_group_id
    )
    AND EXISTS (
      SELECT 1
      FROM latest_batch source
      WHERE source.id = target.id
        AND source.ad_group_id = target.ad_group_id
        AND TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(source.action_items AS STRING), ''),
          COALESCE(source.ad_strength, ''),
          COALESCE(CAST(source.added_by_google_ads AS STRING), ''),
          COALESCE(source.device_preference, ''),
          COALESCE(source.display_url, ''),
          COALESCE(source.final_app_urls, ''),
          COALESCE(source.final_mobile_urls, ''),
          COALESCE(source.final_url_suffix, ''),
          COALESCE(source.final_urls, ''),
          COALESCE(source.name, ''),
          COALESCE(source.policy_summary_approval_status, ''),
          COALESCE(source.policy_summary_review_status, ''),
          COALESCE(source.status, ''),
          COALESCE(source.system_managed_resource_source, ''),
          COALESCE(source.tracking_url_template, ''),
          COALESCE(source.type, ''),
          COALESCE(source.url_collections, '')
        ))) != TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(target.action_items AS STRING), ''),
          COALESCE(target.ad_strength, ''),
          COALESCE(CAST(target.added_by_google_ads AS STRING), ''),
          COALESCE(target.device_preference, ''),
          COALESCE(target.display_url, ''),
          COALESCE(target.final_app_urls, ''),
          COALESCE(target.final_mobile_urls, ''),
          COALESCE(target.final_url_suffix, ''),
          COALESCE(target.final_urls, ''),
          COALESCE(target.name, ''),
          COALESCE(target.policy_summary_approval_status, ''),
          COALESCE(target.policy_summary_review_status, ''),
          COALESCE(target.status, ''),
          COALESCE(target.system_managed_resource_source, ''),
          COALESCE(target.tracking_url_template, ''),
          COALESCE(target.type, ''),
          COALESCE(target.url_collections, '')
        )))
    );

  -- Insert new records
  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    _gn_start,
    ad_group_id,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    action_items,
    ad_strength,
    added_by_google_ads,
    device_preference,
    display_url,
    final_app_urls,
    final_mobile_urls,
    final_url_suffix,
    final_urls,
    name,
    policy_summary_approval_status,
    policy_summary_review_status,
    status,
    system_managed_resource_source,
    tracking_url_template,
    type,
    updated_at,
    url_collections,
    _gn_id
  )
  SELECT 
    _gn_start,
    ad_group_id,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    action_items,
    ad_strength,
    added_by_google_ads,
    device_preference,
    display_url,
    final_app_urls,
    final_mobile_urls,
    final_url_suffix,
    final_urls,
    name,
    policy_summary_approval_status,
    policy_summary_review_status,
    status,
    system_managed_resource_source,
    tracking_url_template,
    type,
    updated_at,
    url_collections,
    _gn_id
  FROM latest_batch source
  WHERE NOT EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}` target
    WHERE target.id = source.id
      AND target.ad_group_id = source.ad_group_id
      AND target._gn_active = TRUE
  );


COMMIT TRANSACTION;
  -- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF;

{% endif %} 