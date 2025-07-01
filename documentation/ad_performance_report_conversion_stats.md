# ad_performance_report_conversion_stats

This table provides conversion statistics for Google Ads at the ad performance level, updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| customer_id                   | INT64     | The numeric ID of the customer/account      |
| ad_id                         | INT64     | Ad ID                                       |
| ad_group_id                   | INT64     | Ad group ID                                 |
| campaign_id                   | INT64     | Campaign ID                                 |
| date                          | DATE      | The date for the reported metrics           |
| conversions                   | FLOAT64   | Number of conversions                       |
| conversions_value             | FLOAT64   | Value of conversions                        |
| all_conversions               | FLOAT64   | All conversions (including cross-device)    |
| all_conversions_value         | FLOAT64   | Value of all conversions                    |
| run_id                        | INT64     | ETL batch run identifier                    |
| _gn_id                        | STRING    | Hash of key attributes for change detection |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key ad, group, campaign, and date attributes.

## Usage

- **Ad performance analysis:** Analyze conversion metrics by ad, group, campaign, and date
- **Trend analysis:** Track changes in conversions and value over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 