# Postal Code Report

This table contains ad group-level performance metrics broken down by postal code, allowing for geographic analysis of ad group performance at a granular level.

## Table Structure

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT64 | The Google Ads customer ID |
| ad_group_id | INT64 | The ad group ID |
| campaign_id | INT64 | The campaign ID |
| geographic_view_resource_name | STRING | The resource name for the geographic view |
| country_criterion_id | STRING | The country criterion ID |
| location_type | STRING | Type of location targeting (e.g., "LOCATION_OF_PRESENCE") |
| clicks | FLOAT64 | Number of clicks |
| conversions | FLOAT64 | Number of conversions |
| conversions_value | FLOAT64 | Value of conversions |
| cost_micros | FLOAT64 | Cost in micros (divide by 1,000,000 for actual cost) |
| impressions | FLOAT64 | Number of impressions |
| all_conversions | FLOAT64 | Total number of conversions across all conversion actions |
| all_conversions_value | FLOAT64 | Total value of all conversions |
| geo_target_city | STRING | The city for the geo target |
| geo_target_postal_code | STRING | The Google Ads geo target code for the postal code (e.g., "geoTargetConstants/9000738") |
| geo_target_state | STRING | The state for the geo target |
| date | DATE | The date of the metrics |
| run_id | INT64 | The ETL batch run identifier |
| tenant | STRING | Tenant identifier |
| _time_extracted | TIMESTAMP | When the record was extracted |
| _time_loaded | TIMESTAMP | When the record was loaded |
| _gn_id | STRING | Unique identifier for the record |
| _gn_synced | TIMESTAMP | When the record was synced |

## Relationships

This table can be joined with other tables in the Google Ads package:

- `account_history`: Join on `customer_id` to get account-level information
- `campaign_history`: Join on `campaign_id` to get campaign details
- `ad_group_history`: Join on `ad_group_id` to get ad group details

## Example Queries

### 1. Ad Group Performance by Postal Code with Ad Group Details

```sql
SELECT 
  p.date,
  a.descriptive_name as account_name,
  c.name as campaign_name,
  ag.name as ad_group_name,
  p.geo_target_postal_code,
  gtc.name as postal_code_name,
  p.location_type,
  p.impressions,
  p.clicks,
  p.cost_micros / 1000000 as cost,
  p.conversions,
  p.conversions_value
FROM `{{target_dataset}}.postal_code_report` p
LEFT JOIN `{{target_dataset}}.account_history` a
  ON p.customer_id = a.customer_id
  AND a._gn_active = TRUE
LEFT JOIN `{{target_dataset}}.campaign_history` c
  ON p.campaign_id = c.id
  AND c._gn_active = TRUE
LEFT JOIN `{{target_dataset}}.ad_group_history` ag
  ON p.ad_group_id = ag.id
  AND ag._gn_active = TRUE
LEFT JOIN `{{target_dataset}}.geo_target_constant` gtc
  ON p.geo_target_postal_code = gtc.resource_name
WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY p.date DESC, p.cost_micros DESC;
```

### 2. Top Performing Postal Codes by Conversion Rate

```sql
SELECT 
  p.geo_target_postal_code,
  SUM(p.impressions) as total_impressions,
  SUM(p.clicks) as total_clicks,
  SUM(p.conversions) as total_conversions,
  SUM(p.cost_micros) / 1000000 as total_cost,
  SUM(p.conversions) / NULLIF(SUM(p.clicks), 0) as conversion_rate
FROM `{{target_dataset}}.postal_code_report` p
WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY p.geo_target_postal_code
HAVING SUM(p.clicks) > 100  -- Filter for meaningful sample size
ORDER BY conversion_rate DESC
LIMIT 10;
```

### 3. Ad Group Performance by Postal Code with Account Status

```sql
SELECT 
  p.date,
  a.status as account_status,
  c.status as campaign_status,
  ag.status as ad_group_status,
  p.geo_target_postal_code,
  SUM(p.impressions) as impressions,
  SUM(p.clicks) as clicks,
  SUM(p.cost_micros) / 1000000 as cost,
  SUM(p.conversions) as conversions
FROM `{{target_dataset}}.postal_code_report` p
LEFT JOIN `{{target_dataset}}.account_history` a
  ON p.customer_id = a.customer_id
  AND a._gn_active = TRUE
LEFT JOIN `{{target_dataset}}.campaign_history` c
  ON p.campaign_id = c.id
  AND c._gn_active = TRUE
LEFT JOIN `{{target_dataset}}.ad_group_history` ag
  ON p.ad_group_id = ag.id
  AND ag._gn_active = TRUE
WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND a.status = 'ENABLED'
  AND c.status = 'ENABLED'
  AND ag.status = 'ENABLED'
GROUP BY p.date, a.status, c.status, ag.status, p.geo_target_postal_code
ORDER BY p.date DESC, cost DESC;
```

## Notes

- The table is updated daily with the latest performance data
- Historical data is preserved, allowing for trend analysis
- Cost is stored in micros (multiply by 1,000,000 to get actual cost)
- When joining with history tables, always include the `_gn_active = TRUE` condition to get the current state
- The `location_type` field can be used to filter for specific types of location targeting
- Campaign IDs and Ad Group IDs are unique across all accounts, so joins to campaign_history and ad_group_history only need the respective IDs
- The `geo_target_postal_code` field contains Google Ads geo target codes (e.g., "geoTargetConstants/9000738") rather than standard postal codes
- To display the human-readable postal code (location name), join `postal_code_report.geo_target_postal_code` to `geo_target_constant.resource_name` and select the `name` field from `geo_target_constant`. 