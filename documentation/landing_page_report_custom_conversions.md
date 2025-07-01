# landing_page_report_custom_conversions

This table provides detailed custom conversion metrics for Google Ads landing pages, supporting granular analysis of conversion actions by landing page and date. It is updated incrementally with each ETL run.

## Table Structure

| Column                  | Type      | Description                                                        |
|-------------------------|-----------|--------------------------------------------------------------------|
| customer_id             | INT64     | The numeric ID of the customer/account                             |
| date                    | DATE      | The date for the reported metrics                                  |
| unexpanded_final_url    | STRING    | The unexpanded final URL of the landing page                       |
| conversions             | FLOAT64   | Number of conversions for the specific conversion action           |
| conversions_value       | FLOAT64   | Value of conversions for the specific action                       |
| all_conversions         | FLOAT64   | All conversions (including cross-device and other types)           |
| all_conversions_value   | FLOAT64   | Value of all conversions                                           |
| conversion_action_name  | STRING    | Name of the conversion action                                      |
| run_id                  | INT64     | ETL batch run identifier                                           |
| _gn_id                  | STRING    | Hash of key attributes for change detection                        |
| _gn_synced              | TIMESTAMP | Timestamp when the record was loaded                               |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- customer_id
- unexpanded_final_url
- date
- conversion_action_name

When any of these attributes change in the source, a new record is inserted for the new batch.

## Usage

- **Custom conversion analysis:** Analyze specific conversion actions by landing page and date
- **Attribution reporting:** Attribute conversions to landing pages and actions
- **Batch tracking:** Use `run_id` to identify data from specific ETL runs

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 