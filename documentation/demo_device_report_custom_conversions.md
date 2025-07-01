# demo_device_report_custom_conversions

This table provides custom conversion metrics for Google Ads by device, supporting granular analysis of conversion actions by device and date. It is updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| customer_id                   | INT64     | The numeric ID of the customer/account      |
| ad_group_id                   | INT64     | Ad group ID                                 |
| campaign_id                   | INT64     | Campaign ID                                 |
| device                        | STRING    | Device type                                 |
| conversion_action_name        | STRING    | Name of the conversion action               |
| conversions                   | FLOAT64   | Number of conversions                       |
| conversions_value             | FLOAT64   | Value of conversions                        |
| all_conversions               | FLOAT64   | All conversions (including cross-device)    |
| all_conversions_value         | FLOAT64   | Value of all conversions                    |
| date                          | DATE      | The date for the reported metrics           |
| run_id                        | INT64     | ETL batch run identifier                    |
| _gn_id                        | STRING    | Hash of key attributes for change detection |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key device, group, campaign, and date attributes.

## Usage

- **Device-based conversion analysis:** Analyze metrics by device, group, campaign, and date
- **Trend analysis:** Track changes in conversions and value over time

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 