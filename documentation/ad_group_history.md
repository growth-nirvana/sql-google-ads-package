# ad_group_history

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Google Ads ad groups, tracking historical changes to ad group attributes over time. It maintains a complete history of ad group changes while providing an easy way to access the current state of each ad group.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| _gn_start                     | TIMESTAMP | Start time of when this version was valid   |
| id                            | INT64     | Ad group ID                                 |
| campaign_id                   | INT64     | Campaign ID                                 |
| name                          | STRING    | Ad group name                               |
| status                        | STRING    | Ad group status                             |
| type                          | STRING    | Ad group type                               |
| cpc_bid_micros                | INT64     | CPC bid in micros                           |
| cpm_bid_micros                | INT64     | CPM bid in micros                           |
| cpv_bid_micros                | INT64     | CPV bid in micros                           |
| target_cpa_micros             | INT64     | Target CPA in micros                        |
| target_roas                   | FLOAT64   | Target ROAS                                 |
| base_ad_group                 | STRING    | Base ad group identifier                    |
| _gn_active                    | BOOL      | Whether this is the current version         |
| _gn_end                       | TIMESTAMP | End time of when this version was valid     |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |
| _gn_id                        | STRING    | Hash of key attributes for change detection |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key ad group and campaign attributes.

## Usage

- **Get current ad group state:** Filter where `_gn_active = TRUE`
- **Track historical changes:** Query without the `_gn_active` filter to see all versions
- **Point-in-time analysis:** Use `_gn_start` and `_gn_end` to see ad group state at any point in time
- **Change analysis:** Compare different versions of the same ad group to see what changed and when

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Google Ads tables through the `id` and `campaign_id` fields
- The `status` field is particularly useful for tracking ad group lifecycle changes 