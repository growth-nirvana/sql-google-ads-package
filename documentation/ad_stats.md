# ad_stats

This table provides detailed statistics for Google Ads at the ad level, including conversions, cost, and engagement metrics. It is updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                                        |
|-------------------------------|-----------|--------------------------------------------------------------------|
| _gn_id                        | STRING    | Hash of key attributes for change detection                        |
| customer_id                   | INT64     | The numeric ID of the customer/account                             |
| date                          | DATE      | The date for the reported metrics                                  |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded                               |
| active_view_impressions       | INT64     | Number of Active View impressions                                  |
| active_view_measurability     | FLOAT64   | Active View measurability rate                                     |
| active_view_measurable_cost_micros | INT64 | Cost in micros for measurable impressions                          |
| active_view_measurable_impressions | INT64 | Number of measurable impressions                                   |
| active_view_viewability       | FLOAT64   | Active View viewability rate                                       |
| ad_group_base_ad_group        | STRING    | Base ad group identifier                                           |
| ad_group_id                   | INT64     | Ad group ID                                                        |
| ad_id                         | INT64     | Ad ID                                                              |
| ad_network_type               | STRING    | Ad network type                                                    |
| campaign_base_campaign        | STRING    | Base campaign identifier                                           |
| campaign_id                   | INT64     | Campaign ID                                                        |
| clicks                        | INT64     | Number of clicks                                                   |
| conversions                   | FLOAT64   | Number of conversions                                              |
| conversions_value             | FLOAT64   | Value of conversions                                               |
| cost_micros                   | INT64     | Cost in micros                                                     |
| cost_per_conversion           | FLOAT64   | Cost per conversion                                                |
| device                        | STRING    | Device type                                                        |
| impressions                   | INT64     | Number of impressions                                              |
| interaction_event_types       | STRING    | Types of interaction events                                        |
| interactions                  | INT64     | Number of interactions                                             |
| video_views                   | INT64     | Number of video views                                              |
| view_through_conversions      | INT64     | Number of view-through conversions                                 |
| run_id                        | INT64     | ETL batch run identifier                                           |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key ad and date attributes.

## Usage

- **Ad performance:** Analyze metrics by ad, date, and device
- **Trend analysis:** Track changes in conversions, cost, and engagement over time
- **Batch tracking:** Use `run_id` to identify data from specific ETL runs

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 