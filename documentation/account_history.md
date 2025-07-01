# account_history

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Google Ads accounts, tracking historical changes to account attributes over time. It maintains a complete history of account changes while providing an easy way to access the current state of each account.

## Table Structure

| Column                        | Type      | Description                                 |
|-------------------------------|-----------|---------------------------------------------|
| _gn_start                     | TIMESTAMP | Start time of when this version was valid   |
| id                            | INT64     | Account ID                                  |
| name                          | STRING    | Account name                                |
| currency_code                 | STRING    | Currency code                               |
| time_zone                     | STRING    | Time zone                                   |
| tracking_url_template         | STRING    | Tracking URL template                       |
| auto_tagging_enabled          | BOOL      | Whether auto-tagging is enabled             |
| status                        | STRING    | Account status                              |
| _gn_active                    | BOOL      | Whether this is the current version         |
| _gn_end                       | TIMESTAMP | End time of when this version was valid     |
| _gn_synced                    | TIMESTAMP | Timestamp when the record was loaded        |
| _gn_id                        | STRING    | Hash of key attributes for change detection |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes key account attributes.

## Usage

- **Get current account state:** Filter where `_gn_active = TRUE`
- **Track historical changes:** Query without the `_gn_active` filter to see all versions
- **Point-in-time analysis:** Use `_gn_start` and `_gn_end` to see account state at any point in time
- **Change analysis:** Compare different versions of the same account to see what changed and when

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Google Ads tables through the `id` field
- The `status` field is particularly useful for tracking account lifecycle changes 