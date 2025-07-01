# campaign_stats

This table provides campaign-level performance statistics for Google Ads, updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| _gn_id                        | STRING    | Hash of key attributes for change detection  |
| customer_id                   | INT64     | The numeric ID of the customer/account      |
| date                          | DATE      | The date for the reported metrics           |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |
| active_view_impressions       | INT64     | Number of Active View impressions           |
| active_view_measurability     | FLOAT64   | Active View measurability rate              |
| active_view_measurable_cost_micros | INT64 | Cost in micros for measurable impressions   |
| active_view_measurable_impressions | INT64 | Number of measurable impressions            |
| active_view_viewability       | FLOAT64   | Active View viewability rate                |
| ad_network_type               | STRING    | Ad network type                             |
| base_campaign                 | STRING    | Base campaign identifier                    |
| clicks                        | INT64     | Number of clicks                            |
| conversions                   | FLOAT64   | Number of conversions                       |
| conversions_value             | FLOAT64   | Value of conversions                        |
| cost_micros                   | INT64     | Cost in micros                              |
| device                        | STRING    | Device type                                 |
| id                            | INT64     | Campaign ID                                 |
| impressions                   | INT64     | Number of impressions                       |
| interaction_event_types       | STRING    | Types of interaction events                 |
| interactions                  | INT64     | Number of interactions                      |
| view_through_conversions      | INT64     | Number of view-through conversions          |
| run_id                        | INT64     | ETL batch run identifier                    |

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