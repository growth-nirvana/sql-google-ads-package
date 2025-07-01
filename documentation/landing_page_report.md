# landing_page_report

This table provides a snapshot of Google Ads landing page performance metrics, updated incrementally with each ETL run. It is designed for point-in-time and trend analysis of landing page effectiveness, including conversions, cost, and speed score.

## Table Structure

| Column                  | Type      | Description                                                        |
|-------------------------|-----------|--------------------------------------------------------------------|
| customer_id             | INT64     | The numeric ID of the customer/account                             |
| date                    | DATE      | The date for the reported metrics                                  |
| unexpanded_final_url    | STRING    | The unexpanded final URL of the landing page                       |
| clicks                  | INT64     | Number of clicks on the landing page                               |
| impressions             | INT64     | Number of impressions for the landing page                         |
| conversions             | FLOAT64   | Number of conversions attributed to the landing page               |
| all_conversions         | FLOAT64   | All conversions (including cross-device and other types)           |
| conversions_value       | FLOAT64   | Value of conversions                                               |
| all_conversions_value   | FLOAT64   | Value of all conversions                                           |
| cost_micros             | INT64     | Cost in micros (1,000,000 micros = 1 unit of currency)             |
| speed_score             | STRING    | Google Ads landing page speed score                                |
| run_id                  | INT64     | ETL batch run identifier                                          |
| _gn_id                  | STRING    | Hash of key attributes for change detection                        |
| _gn_synced              | TIMESTAMP | Timestamp when the record was loaded                               |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- customer_id
- unexpanded_final_url
- date

When any of these attributes change in the source, a new record is inserted for the new batch.

## Usage

- **Landing page performance:** Analyze metrics by URL and date
- **Trend analysis:** Track changes in conversions, cost, and speed score over time
- **Batch tracking:** Use `run_id` to identify data from specific ETL runs

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 