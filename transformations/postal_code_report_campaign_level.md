# Postal Code Report (Campaign Level)

This table contains campaign-level performance metrics broken down by postal code, allowing for geographic analysis of campaign performance at a granular level.

## Table Structure

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT64 | The Google Ads customer ID |
| campaign_id | INT64 | The campaign ID |
| location_type | STRING | Type of location targeting (e.g., "LOCATION_OF_PRESENCE") |
| clicks | INT64 | Number of clicks |
| conversions | FLOAT64 | Number of conversions |
| conversions_value | FLOAT64 | Value of conversions |
| cost_micros | INT64 | Cost in micros (divide by 1,000,000 for actual cost) |
| impressions | INT64 | Number of impressions |
| all_conversions | FLOAT64 | Total number of conversions across all conversion actions |
| all_conversions_value | FLOAT64 | Total value of all conversions |
| geo_target_postal_code | STRING | The Google Ads geo target code for the postal code (e.g., "geoTargetConstants/9000738") |
| date | DATE | The date of the metrics |
| _fivetran_id | STRING | Unique identifier for the record |
| _fivetran_synced | TIMESTAMP | When the record was synced |

## Relationships

This table can be joined with other tables in the Google Ads package:

- `account_history`: Join on `customer_id` to get account-level information
- `campaign_history`: Join on `campaign_id` to get campaign details (campaign_id is unique across all accounts)

## Example Queries

### 1. Campaign Performance by Postal Code with Campaign Details

```sql
SELECT 
  p.date,
  a.descriptive_name as account_name,
  c.name as campaign_name,
  p.geo_target_postal_code,
  gtc.name as postal_code_name,
  p.location_type,
  p.impressions,
  p.clicks,
  p.cost_micros / 1000000 as cost,
  p.conversions,
  p.conversions_value
FROM `{{target_dataset}}.postal_code_report_campaign_level` p
LEFT JOIN `{{target_dataset}}.account_history` a
  ON p.customer_id = a.customer_id
  AND a._fivetran_active = TRUE
LEFT JOIN `{{target_dataset}}.campaign_history` c
  ON p.campaign_id = c.id
  AND c._fivetran_active = TRUE
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
FROM `{{target_dataset}}.postal_code_report_campaign_level` p
WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY p.geo_target_postal_code
HAVING SUM(p.clicks) > 100  -- Filter for meaningful sample size
ORDER BY conversion_rate DESC
LIMIT 10;
```

### 3. Campaign Performance by Postal Code with Account Status

```sql
SELECT 
  p.date,
  a.status as account_status,
  c.status as campaign_status,
  p.geo_target_postal_code,
  SUM(p.impressions) as impressions,
  SUM(p.clicks) as clicks,
  SUM(p.cost_micros) / 1000000 as cost,
  SUM(p.conversions) as conversions
FROM `{{target_dataset}}.postal_code_report_campaign_level` p
LEFT JOIN `{{target_dataset}}.account_history` a
  ON p.customer_id = a.customer_id
  AND a._fivetran_active = TRUE
LEFT JOIN `{{target_dataset}}.campaign_history` c
  ON p.campaign_id = c.id
  AND c._fivetran_active = TRUE
WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND a.status = 'ENABLED'
  AND c.status = 'ENABLED'
GROUP BY p.date, a.status, c.status, p.geo_target_postal_code
ORDER BY p.date DESC, cost DESC;
```

## Notes

- The table is updated daily with the latest performance data
- Historical data is preserved, allowing for trend analysis
- Cost is stored in micros (multiply by 1,000,000 to get actual cost)
- When joining with history tables, always include the `_fivetran_active = TRUE` condition to get the current state
- The `location_type` field can be used to filter for specific types of location targeting
- Campaign IDs are unique across all accounts, so joins to campaign_history only need the campaign_id
- The `geo_target_postal_code` field contains Google Ads geo target codes (e.g., "geoTargetConstants/9000738") rather than standard postal codes
- To display the human-readable postal code (location name), join `postal_code_report_campaign_level.geo_target_postal_code` to `geo_target_constant.resource_name` and select the `name` field from `geo_target_constant`. 