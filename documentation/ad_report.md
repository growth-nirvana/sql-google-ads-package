# ad_report

This table provides a detailed report of Google Ads at the ad level, including campaign, ad group, and ad attributes, as well as performance metrics. It is updated incrementally with each ETL run.

## Table Structure

| Column                        | Type      | Description                                                        |
|-------------------------------|-----------|--------------------------------------------------------------------|
| customer_id                   | INT64     | The numeric ID of the customer/account                             |
| date                          | DATE      | The date for the reported metrics                                  |
| campaign_id                   | INT64     | Campaign ID                                                        |
| campaign_name                 | STRING    | Campaign name                                                      |
| campaign_status               | STRING    | Campaign status                                                    |
| ad_group_id                   | INT64     | Ad group ID                                                        |
| ad_group_name                 | STRING    | Ad group name                                                      |
| ad_group_status               | STRING    | Ad group status                                                    |
| ad_id                         | INT64     | Ad ID                                                              |
| ad_added_by_google_ads        | BOOL      | Whether the ad was added by Google Ads                             |
| ad_device_preference          | STRING    | Device preference for the ad                                       |
| ad_display_url                | STRING    | Display URL for the ad                                             |
| ad_final_mobile_urls          | STRING    | Final mobile URLs for the ad                                       |
| ad_final_urls                 | STRING    | Final URLs for the ad                                              |
| ad_tracking_url_template      | STRING    | Tracking URL template                                              |
| ad_url_custom_parameters      | STRING    | Custom URL parameters                                              |
| ad_type                       | STRING    | Ad type                                                            |
| expanded_text_ad_description  | STRING    | Expanded text ad description                                       |
| expanded_text_ad_description_2| STRING    | Expanded text ad description 2                                     |
| expanded_text_ad_headline_part_1 | STRING | Expanded text ad headline part 1                                   |
| expanded_text_ad_headline_part_2 | STRING | Expanded text ad headline part 2                                   |
| expanded_text_ad_headline_part_3 | STRING | Expanded text ad headline part 3                                   |
| expanded_text_ad_path_1       | STRING    | Expanded text ad path 1                                            |
| expanded_text_ad_path_2       | STRING    | Expanded text ad path 2                                            |
| legacy_responsive_display_ad_call_to_action_text | STRING | Call to action text for legacy responsive display ad               |
| legacy_responsive_display_ad_description | STRING | Description for legacy responsive display ad                       |
| text_ad_description_1         | STRING    | Text ad description 1                                              |
| ad_text_ad_description_2      | STRING    | Text ad description 2                                              |
| text_ad_headline              | STRING    | Text ad headline                                                   |
| call_ad_description_1         | STRING    | Call ad description 1                                              |
| call_ad_description_2         | STRING    | Call ad description 2                                              |
| labels                        | STRING    | Labels associated with the ad                                      |
| policy_summary_approval_status| STRING    | Policy summary approval status                                     |
| status                        | STRING    | Ad status                                                          |
| all_conversions               | FLOAT64   | All conversions (including cross-device and other types)           |
| all_conversions_value         | FLOAT64   | Value of all conversions                                           |
| conversions                   | FLOAT64   | Number of conversions                                              |
| conversions_value             | FLOAT64   | Value of conversions                                               |
| clicks                        | INT64     | Number of clicks                                                   |
| cost_micros                   | INT64     | Cost in micros                                                     |
| impressions                   | INT64     | Number of impressions                                              |
| view_through_conversions      | INT64     | Number of view-through conversions                                 |
| _gn_id                        | STRING    | Hash of key attributes for change detection                        |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded                               |
| run_id                        | INT64     | ETL batch run identifier                                           |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key ad and date attributes.

## Usage

- **Ad performance:** Analyze metrics by ad, campaign, and ad group
- **Trend analysis:** Track changes in conversions, cost, and engagement over time
- **Batch tracking:** Use `run_id` to identify data from specific ETL runs

## Notes

- The table is updated incrementally, only processing the latest batch per run
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table is designed for reporting and analytics, not for SCD Type 2 history 