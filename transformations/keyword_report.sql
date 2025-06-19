-- keyword_report
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'keyword_report' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'stream_keyword_report' %}
{% assign drop_source_table = vars.drop_source_table | default: false %}

{% if vars.models.switchover_keyword_report.active == false %}
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
  customer_id INT64,
  date DATE,
  campaign_id INT64,
  ad_group_id INT64,
  ad_group_criterion_criterion_id INT64,
  keyword_text STRING,
  keyword_match_type STRING,
  ad_group_criterion_labels STRING,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  clicks INT64,
  cost_micros INT64,
  impressions INT64,
  view_through_conversions INT64,
  absolute_top_impression_percentage FLOAT64,
  search_budget_lost_absolute_top_impression_share FLOAT64,
  search_budget_lost_top_impression_share FLOAT64,
  search_impression_share FLOAT64,
  search_rank_lost_absolute_top_impression_share FLOAT64,
  search_rank_lost_impression_share FLOAT64,
  search_rank_lost_top_impression_share FLOAT64,
  search_top_impression_share FLOAT64,
  top_impression_percentage FLOAT64,
  search_absolute_top_impression_share FLOAT64,
  historical_creative_quality_score STRING,
  historical_landing_page_quality_score STRING,
  historical_quality_score INT64,
  historical_search_predicted_ctr STRING,
  _gn_id STRING,
  _gn_synced TIMESTAMP,
  run_id INT64
);

-- Step 1: Create temp table for latest batch using run_id
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
    ad_group_id,
    ad_group_criterion_criterion_id,
    keyword_text,
    keyword_match_type,
    ad_group_criterion_labels,
    all_conversions,
    all_conversions_value,
    conversions,
    conversions_value,
    clicks,
    cost_micros,
    impressions,
    view_through_conversions,
    absolute_top_impression_percentage,
    search_budget_lost_absolute_top_impression_share,
    search_budget_lost_top_impression_share,
    search_impression_share,
    search_rank_lost_absolute_top_impression_share,
    search_rank_lost_impression_share,
    search_rank_lost_top_impression_share,
    search_top_impression_share,
    top_impression_percentage,
    search_absolute_top_impression_share,
    historical_creative_quality_score,
    historical_landing_page_quality_score,
    historical_quality_score,
    historical_search_predicted_ctr,
    _gn_id,
    _gn_synced,
    run_id
  )
  SELECT 
    CAST(customer__id AS INT64),
    DATE(segments__date),
    CAST(campaign__id AS INT64),
    CAST(adGroup__id AS INT64),
    CAST(adGroupCriterion__criterionId AS INT64),
    adGroupCriterion__keyword__text,
    adGroupCriterion__keyword__matchType,
    CAST(NULL AS STRING),
    SAFE_CAST(metrics__allConversions AS FLOAT64),
    SAFE_CAST(metrics__allConversionsValue AS FLOAT64),
    SAFE_CAST(metrics__conversions AS FLOAT64),
    SAFE_CAST(metrics__conversionsValue AS FLOAT64),
    SAFE_CAST(metrics__clicks AS INT64),
    SAFE_CAST(metrics__costMicros AS INT64),
    SAFE_CAST(metrics__impressions AS INT64),
    SAFE_CAST(metrics__viewThroughConversions AS INT64),
    SAFE_CAST(metrics__absoluteTopImpressionPercentage AS FLOAT64),
    SAFE_CAST(metrics__searchBudgetLostAbsoluteTopImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchBudgetLostTopImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchRankLostAbsoluteTopImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchRankLostImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchRankLostTopImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__searchTopImpressionShare AS FLOAT64),
    SAFE_CAST(metrics__topImpressionPercentage AS FLOAT64),
    SAFE_CAST(metrics__searchAbsoluteTopImpressionShare AS FLOAT64),
    metrics__historicalCreativeQualityScore,
    metrics__historicalLandingPageQualityScore,
    SAFE_CAST(metrics__historicalQualityScore AS INT64),
    metrics__historicalSearchPredictedCtr,
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer__id AS STRING), ''),
      COALESCE(CAST(campaign__id AS STRING), ''),
      COALESCE(segments__date, ''),
      COALESCE(CAST(adGroup__id AS STRING), ''),
      COALESCE(CAST(adGroupCriterion__criterionId AS STRING), ''),
      COALESCE(adGroupCriterion__keyword__text, ''),
      COALESCE(adGroupCriterion__keyword__matchType, ''),
      COALESCE(CAST(NULL AS STRING), ''),
      COALESCE(metrics__historicalCreativeQualityScore, ''),
      COALESCE(metrics__historicalLandingPageQualityScore, ''),
      COALESCE(CAST(metrics__historicalQualityScore AS STRING), ''),
      COALESCE(metrics__historicalSearchPredictedCtr, '')
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