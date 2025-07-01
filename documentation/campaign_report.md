# campaign_report

This table provides a detailed report of Google Ads campaigns, including performance metrics and campaign attributes. It is updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| customer_id                   | INT64     | The numeric ID of the customer/account      |
| date                          | DATE      | The date for the reported metrics           |
| id                            | INT64     | Campaign ID                                 |
| name                          | STRING    | Campaign name                               |
| status                        | STRING    | Campaign status                             |
| advertising_channel_type      | STRING    | Advertising channel type                    |
| advertising_channel_sub_type  | STRING    | Advertising channel sub type                |
| serving_status                | STRING    | Serving status                              |
| start_date                    | DATE      | Campaign start date                         |
| end_date                      | DATE      | Campaign end date                           |
| budget_id                     | INT64     | Budget ID                                   |
| budget                        | FLOAT64   | Budget amount                               |
| clicks                        | INT64     | Number of clicks                            |
| impressions                   | INT64     | Number of impressions                       |
| cost_micros                   | INT64     | Cost in micros                              |
| conversions                   | FLOAT64   | Number of conversions                       |
| conversions_value             | FLOAT64   | Value of conversions                        |
| all_conversions               | FLOAT64   | All conversions (including cross-device)    |
| all_conversions_value         | FLOAT64   | Value of all conversions                    |
| run_id                        | INT64     | ETL batch run identifier                    |
| _gn_id                        | STRING    | Hash of key attributes for change detection |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key campaign and date attributes.

## Usage

- **Campaign performance:** Analyze metrics by campaign and date
- **Trend analysis:** Track changes in conversions, cost, and engagement over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 