-- account_history
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'account_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_account' %}

{% if vars.models.switchover_account_history.active == false %}
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
  auto_tagging_enabled BOOL,
  call_reporting_enabled BOOL,
  conversion_tracking_setting__conversion_tracking_id INT64,
  conversion_tracking_setting__cross_account_conversion_tracking_id INT64,
  currency_code STRING,
  descriptive_name STRING,
  final_url_suffix STRING,
  manager_customer_id INT64,
  pay_per_conversion_eligibility_failure_reasons STRING,
  resource_name STRING,
  status STRING,
  time_zone STRING,
  tracking_url_template STRING,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch with deduplication
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT *
  FROM `{{source_dataset}}.{{source_table_id}}`
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  CAST(customer__id AS INT64) AS id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  TIMESTAMP '9999-01-01' AS updated_at,
  CAST(customer__id AS INT64) AS customer_id,
  CAST(NULL AS BOOL) AS auto_tagging_enabled,
  CAST(NULL AS BOOL) AS call_reporting_enabled,
  CAST(NULL AS INT64) AS conversion_tracking_setting__conversion_tracking_id,
  CAST(NULL AS INT64) AS conversion_tracking_setting__cross_account_conversion_tracking_id,
  customer__currencyCode AS currency_code,
  customer__descriptiveName AS descriptive_name,
  CAST(NULL AS STRING) AS final_url_suffix,
  CAST(NULL AS INT64) AS manager_customer_id,
  CAST(NULL AS STRING) AS pay_per_conversion_eligibility_failure_reasons,
  customer__status AS status,
  customer__timeZone AS time_zone,
  CAST(NULL AS STRING) AS tracking_url_template,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(customer__id AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    COALESCE(CAST(NULL AS STRING), ''),
    customer__currencyCode,
    customer__descriptiveName,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    customer__status,
    customer__timeZone,
    CAST(NULL AS STRING)
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
          COALESCE(CAST(source.auto_tagging_enabled AS STRING), ''),
          COALESCE(CAST(source.call_reporting_enabled AS STRING), ''),
          COALESCE(CAST(source.conversion_tracking_setting__conversion_tracking_id AS STRING), ''),
          COALESCE(CAST(source.conversion_tracking_setting__cross_account_conversion_tracking_id AS STRING), ''),
          COALESCE(source.currency_code, ''),
          COALESCE(source.descriptive_name, ''),
          COALESCE(source.final_url_suffix, ''),
          COALESCE(CAST(source.manager_customer_id AS STRING), ''),
          COALESCE(source.pay_per_conversion_eligibility_failure_reasons, ''),
          COALESCE(source.status, ''),
          COALESCE(source.time_zone, ''),
          COALESCE(source.tracking_url_template, '')
        ))) != TO_HEX(SHA256(CONCAT(
          COALESCE(CAST(target.customer_id AS STRING), ''),
          COALESCE(CAST(target.auto_tagging_enabled AS STRING), ''),
          COALESCE(CAST(target.call_reporting_enabled AS STRING), ''),
          COALESCE(CAST(target.conversion_tracking_setting__conversion_tracking_id AS STRING), ''),
          COALESCE(CAST(target.conversion_tracking_setting__cross_account_conversion_tracking_id AS STRING), ''),
          COALESCE(target.currency_code, ''),
          COALESCE(target.descriptive_name, ''),
          COALESCE(target.final_url_suffix, ''),
          COALESCE(CAST(target.manager_customer_id AS STRING), ''),
          COALESCE(target.pay_per_conversion_eligibility_failure_reasons, ''),
          COALESCE(target.status, ''),
          COALESCE(target.time_zone, ''),
          COALESCE(target.tracking_url_template, '')
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
    auto_tagging_enabled,
    call_reporting_enabled,
    conversion_tracking_setting__conversion_tracking_id,
    conversion_tracking_setting__cross_account_conversion_tracking_id,
    currency_code,
    descriptive_name,
    final_url_suffix,
    manager_customer_id,
    pay_per_conversion_eligibility_failure_reasons,
    status,
    time_zone,
    tracking_url_template
  )
  SELECT 
    _gn_start,
    id,
    _gn_active,
    _gn_end,
    _gn_synced,
    updated_at,
    customer_id,
    auto_tagging_enabled,
    call_reporting_enabled,
    conversion_tracking_setting__conversion_tracking_id,
    conversion_tracking_setting__cross_account_conversion_tracking_id,
    currency_code,
    descriptive_name,
    final_url_suffix,
    manager_customer_id,
    pay_per_conversion_eligibility_failure_reasons,
    status,
    time_zone,
    tracking_url_template
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