# country_report

This table provides country-level segmentation metrics for Google Ads, updated incrementally with each ETL run.

## Table Structure

| Column                | Type      | Description                                 |
|-----------------------|-----------|---------------------------------------------|
| _gn_id                | STRING    | Hash of key attributes for change detection  |
| customer_id           | INT64     | The numeric ID of the customer/account      |
| campaign_id           | INT64     | Campaign ID                                 |
| ad_group_id           | INT64     | Ad group ID                                 |
| country_criterion_id  | STRING    | Country criterion ID                        |
| location_type         | STRING    | Location type                               |
| date                  | DATE      | The date for the reported metrics           |
| _gn_synced            | TIMESTAMP | Timestamp when the record was loaded        |
| clicks                | FLOAT64   | Number of clicks                            |
| conversions           | FLOAT64   | Number of conversions                       |
| conversions_value     | FLOAT64   | Value of conversions                        |
| impressions           | FLOAT64   | Number of impressions                       |
| cost_micros           | FLOAT64   | Cost in micros                              |
| tenant                | STRING    | Tenant identifier                           |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key country, group, and date attributes.

## Usage

- **Country segmentation:** Analyze metrics by country and date
- **Trend analysis:** Track changes in conversions, cost, and engagement over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 