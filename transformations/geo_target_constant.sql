-- geo_target_constant
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'geo_target_constant' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_geo_target_constant' %}

{% if vars.models.switchover_geo_target_constant.active == false %}
select 1
{% else %}

DECLARE min_time TIMESTAMP;
DECLARE max_time TIMESTAMP;
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
  customer_id STRING,
  resource_name STRING,
  status STRING,
  id STRING,
  name STRING,
  country_code STRING,
  target_type STRING,
  canonical_name STRING,
  tenant STRING,
  _gn_id STRING,
  _gn_synced TIMESTAMP
);

-- Step 1: Create temp table for latest batch using run_id
CREATE TEMP TABLE latest_batch AS
SELECT *
FROM `{{source_dataset}}.{{source_table_id}}`
WHERE run_id = (
  SELECT MAX(run_id)
  FROM `{{source_dataset}}.{{source_table_id}}`
);

-- Step 2: Assign min/max times using SET + scalar subqueries
SET min_time = (
  SELECT MIN(_time_extracted) FROM latest_batch
);

SET max_time = (
  SELECT MAX(_time_extracted) FROM latest_batch
);

-- Step 3: Conditional delete and insert
BEGIN TRANSACTION;

  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE _gn_synced BETWEEN min_time AND max_time
      AND customer_id IN (
        SELECT DISTINCT customer_id
        FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset}}.{{target_table_id}}`
    WHERE _gn_synced BETWEEN min_time AND max_time
      AND customer_id IN (
        SELECT DISTINCT customer_id
        FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    customer_id,
    resource_name,
    status,
    id,
    name,
    country_code,
    target_type,
    canonical_name,
    tenant,
    _gn_id,
    _gn_synced
  )
  SELECT 
    customer_id,
    geoTargetConstant__resourceName,
    geoTargetConstant__status,
    geoTargetConstant__id,
    geoTargetConstant__name,
    geoTargetConstant__countryCode,
    geoTargetConstant__targetType,
    geoTargetConstant__canonicalName,
    tenant,
    TO_HEX(SHA256(CONCAT(
      COALESCE(customer_id, ''),
      COALESCE(geoTargetConstant__resourceName, ''),
      COALESCE(geoTargetConstant__status, ''),
      COALESCE(geoTargetConstant__id, ''),
      COALESCE(geoTargetConstant__name, ''),
      COALESCE(geoTargetConstant__countryCode, ''),
      COALESCE(geoTargetConstant__targetType, ''),
      COALESCE(geoTargetConstant__canonicalName, ''),
      COALESCE(tenant, '')
    ))) AS _gn_id,
    CURRENT_TIMESTAMP()
  FROM latest_batch;

COMMIT TRANSACTION;
-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF;

{% endif %} 