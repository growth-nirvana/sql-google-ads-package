# campaign_history

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Google Ads campaigns, tracking historical changes to campaign attributes over time. It maintains a complete history of campaign changes while providing an easy way to access the current state of each campaign.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| _gn_start                     | TIMESTAMP | Start time of when this version was valid   |
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
| _gn_active                    | BOOL      | Whether this is the current version         |
| _gn_end                       | TIMESTAMP | End time of when this version was valid     |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |
| _gn_id                        | STRING    | Hash of key attributes for change detection |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key campaign attributes.

## Usage

- **Get current campaign state:** Filter where `_gn_active = TRUE`
- **Track historical changes:** Query without the `_gn_active` filter to see all versions
- **Point-in-time analysis:** Use `_gn_start` and `_gn_end` to see campaign state at any point in time
- **Change analysis:** Compare different versions of the same campaign to see what changed and when

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Google Ads tables through the `id` field
- The `status` field is particularly useful for tracking campaign lifecycle changes 