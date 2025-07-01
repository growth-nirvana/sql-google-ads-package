# gender_report

This table provides gender-based segmentation metrics for Google Ads, updated incrementally with each ETL run.

## Table Structure

| Column                | Type      | Description                                 |
|-----------------------|-----------|---------------------------------------------|
| _gn_id                | STRING    | Hash of key attributes for change detection  |
| customer_id           | INT64     | The numeric ID of the customer/account      |
| ad_group_id           | INT64     | Ad group ID                                 |
| criterion_id          | INT64     | Criterion ID                                |
| gender_type           | STRING    | Gender type                                 |
| campaign_id           | INT64     | Campaign ID                                 |
| date                  | DATE      | The date for the reported metrics           |
| _gn_synced            | TIMESTAMP | Timestamp when the record was loaded        |
| clicks                | FLOAT64   | Number of clicks                            |
| conversions           | FLOAT64   | Number of conversions                       |
| conversions_value     | FLOAT64   | Value of conversions                        |
| cost_micros           | FLOAT64   | Cost in micros                              |
| impressions           | FLOAT64   | Number of impressions                       |
| tenant                | STRING    | Tenant identifier                           |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key gender, group, and date attributes.

## Usage

- **Gender segmentation:** Analyze metrics by gender and date
- **Trend analysis:** Track changes in conversions, cost, and engagement over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 