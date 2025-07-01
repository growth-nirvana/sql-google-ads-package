# keyword_report

This table provides keyword-level performance metrics for Google Ads, updated incrementally with each ETL run.

## Table Structure

| Column                              | Type      | Description                                 |
|------------------------------------- |-----------|---------------------------------------------|
| customer_id                         | INT64     | The numeric ID of the customer/account      |
| date                                | DATE      | The date for the reported metrics           |
| campaign_id                         | INT64     | Campaign ID                                 |
| ad_group_id                         | INT64     | Ad group ID                                 |
| ad_group_criterion_criterion_id      | INT64     | Ad group criterion ID                       |
| keyword_text                        | STRING    | Keyword text                                |
| keyword_match_type                  | STRING    | Keyword match type                          |
| ad_group_criterion_labels           | STRING    | Ad group criterion labels                   |
| all_conversions                     | FLOAT64   | All conversions (including cross-device)    |
| all_conversions_value               | FLOAT64   | Value of all conversions                    |
| conversions                         | FLOAT64   | Number of conversions                       |
| conversions_value                   | FLOAT64   | Value of conversions                        |
| clicks                              | INT64     | Number of clicks                            |
| cost_micros                         | INT64     | Cost in micros                              |
| impressions                         | INT64     | Number of impressions                       |
| view_through_conversions            | INT64     | Number of view-through conversions          |
| absolute_top_impression_percentage  | FLOAT64   | Absolute top impression percentage          |
| search_budget_lost_absolute_top_impression_share | FLOAT64 | Search budget lost absolute top impression share |
| search_budget_lost_top_impression_share | FLOAT64 | Search budget lost top impression share     |
| search_impression_share             | FLOAT64   | Search impression share                     |
| search_rank_lost_absolute_top_impression_share | FLOAT64 | Search rank lost absolute top impression share |
| search_rank_lost_impression_share   | FLOAT64   | Search rank lost impression share           |
| search_rank_lost_top_impression_share | FLOAT64 | Search rank lost top impression share       |
| search_top_impression_share         | FLOAT64   | Search top impression share                 |
| top_impression_percentage           | FLOAT64   | Top impression percentage                   |
| search_absolute_top_impression_share| FLOAT64   | Search absolute top impression share        |
| historical_creative_quality_score   | STRING    | Historical creative quality score           |
| historical_landing_page_quality_score | STRING  | Historical landing page quality score       |
| historical_quality_score            | INT64     | Historical quality score                    |
| historical_search_predicted_ctr     | STRING    | Historical search predicted CTR             |
| _gn_id                              | STRING    | Hash of key attributes for change detection |
| _gn_synced                          | TIMESTAMP | Timestamp when the record was loaded        |
| run_id                              | INT64     | ETL batch run identifier                    |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key keyword, group, and date attributes.

## Usage

- **Keyword performance:** Analyze metrics by keyword and date
- **Trend analysis:** Track changes in conversions, cost, and engagement over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 