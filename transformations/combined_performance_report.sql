-- Combined Performance Report Transformation
{% assign target_dataset = vars.combined_report_dataset_id %}
{% assign target_table_id = 'combined_performance_report' %}

{% assign source_dataset = vars.target_dataset_id %}

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  date DATE,
  account_id STRING,
  account_name STRING,
  campaign_id STRING,
  campaign_name STRING,
  spend FLOAT64,
  clicks INT64,
  impressions INT64,
  conversions FLOAT64,
  channel STRING,
  segment STRING,
  run_id INT64,
  _gn_id STRING,
  _gn_synced TIMESTAMP
)
PARTITION BY date
CLUSTER BY account_id,segment;

-- Perform MERGE operation for Google Ads data
MERGE `{{target_dataset}}.{{target_table_id}}` T
USING (
  WITH latest_accounts AS (
    SELECT 
      id,
      descriptive_name as name
    FROM `{{source_dataset}}.account_history`
    WHERE _gn_active = true
  ),
  latest_campaigns AS (
    SELECT 
      id,
      name
    FROM `{{source_dataset}}.campaign_history`
    WHERE _gn_active = true
  )
  SELECT 
    report.date,
    'GOOGLE_ADS' as channel,
    'PERFORMANCE' as segment,
    CAST(report.customer_id AS STRING) as account_id,
    la.name as account_name,
    CAST(report.id AS STRING) as campaign_id,
    lc.name as campaign_name,
    report.run_id,
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(report.customer_id AS STRING), ''),
      COALESCE(CAST(report.id AS STRING), ''),
      COALESCE(CAST(report.date AS STRING), '')
    ))) AS _gn_id,
    CURRENT_TIMESTAMP() AS _gn_synced,
    SUM(SAFE_CAST(report.cost_micros AS FLOAT64) / 1000000) as spend,
    SUM(report.clicks) as clicks,
    SUM(report.impressions) as impressions,
    SUM(report.conversions) as conversions
  FROM `{{source_dataset}}.campaign_report` report
  LEFT JOIN latest_accounts la
    ON SAFE_CAST(report.customer_id AS STRING) = SAFE_CAST(la.id AS STRING)
  LEFT JOIN latest_campaigns lc
    ON SAFE_CAST(report.id AS STRING) = SAFE_CAST(lc.id AS STRING)
  GROUP BY 
    1,2,3,4,5,6,7,8,9,10
) S
ON T.date = S.date 
  AND T.account_id = S.account_id 
  AND T.campaign_id = S.campaign_id
  AND T.channel = S.channel
WHEN MATCHED THEN
  UPDATE SET
    account_name = S.account_name,
    campaign_name = S.campaign_name,
    spend = S.spend,
    clicks = S.clicks,
    impressions = S.impressions,
    conversions = S.conversions,
    segment = S.segment,
    run_id = S.run_id,
    _gn_id = S._gn_id,
    _gn_synced = S._gn_synced
WHEN NOT MATCHED THEN
  INSERT (
    date, account_id, account_name, campaign_id, campaign_name,
    spend, clicks, impressions, conversions, channel, segment,
    run_id, _gn_id, _gn_synced
  )
  VALUES (
    S.date, S.account_id, S.account_name, S.campaign_id, S.campaign_name,
    S.spend, S.clicks, S.impressions, S.conversions, S.channel, S.segment,
    S.run_id, S._gn_id, S._gn_synced
  ); 