# ad_history

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Google Ads, tracking historical changes to ad attributes over time. It maintains a complete history of ad changes while providing an easy way to access the current state of each ad.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| _gn_start                     | TIMESTAMP | Start time of when this version was valid   |
| ad_group_id                   | INT64     | Ad group ID                                 |
| id                            | INT64     | Ad ID                                       |
| _gn_active                    | BOOL      | Whether this is the current version         |
| _gn_end                       | TIMESTAMP | End time of when this version was valid     |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |
| action_items                  | STRING    | Action items                                |
| ad_strength                   | STRING    | Ad strength                                 |
| added_by_google_ads           | BOOL      | Whether the ad was added by Google Ads      |
| device_preference             | STRING    | Device preference                           |
| display_url                   | STRING    | Display URL                                 |
| final_app_urls                | STRING    | Final app URLs                              |
| final_mobile_urls             | STRING    | Final mobile URLs                           |
| final_url_suffix              | STRING    | Final URL suffix                            |
| final_urls                    | STRING    | Final URLs                                  |
| name                          | STRING    | Ad name                                     |
| policy_summary_approval_status| STRING    | Policy summary approval status              |
| policy_summary_review_status  | STRING    | Policy summary review status                |
| status                        | STRING    | Ad status                                   |
| system_managed_resource_source| STRING    | System managed resource source              |
| tracking_url_template         | STRING    | Tracking URL template                       |
| type                          | STRING    | Ad type                                     |
| updated_at                    | TIMESTAMP | Timestamp when the ad was last updated      |
| url_collections               | STRING    | URL collections                             |
| _gn_id                        | STRING    | Hash of key attributes for change detection |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key ad and group attributes.

## Usage

- **Get current ad state:** Filter where `_gn_active = TRUE`
- **Track historical changes:** Query without the `_gn_active` filter to see all versions
- **Point-in-time analysis:** Use `_gn_start` and `_gn_end` to see ad state at any point in time
- **Change analysis:** Compare different versions of the same ad to see what changed and when

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Google Ads tables through the `id` and `ad_group_id` fields
- The `status` field is particularly useful for tracking ad lifecycle changes 