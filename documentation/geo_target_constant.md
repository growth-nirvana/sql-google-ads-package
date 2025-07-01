# Geo Target Constant Table

This table contains reference data for Google Ads geo target constants, which describe geographic locations (such as cities, airports, regions, etc.) used for targeting in Google Ads campaigns. It is useful for joining with reporting tables to provide human-readable location names and metadata.

## Table Structure

| Column         | Type      | Description                                                                                  |
|----------------|-----------|----------------------------------------------------------------------------------------------|
| customer_id    | STRING    | The Google Ads customer ID                                                                  |
| resource_name  | STRING    | The resource name of the geo target constant (e.g., `geoTargetConstants/9041140`)           |
| status         | STRING    | The status of the geo target constant (e.g., `ENABLED`, `REMOVAL_PLANNED`)                  |
| id             | STRING    | The unique ID of the geo target constant                                                    |
| name           | STRING    | The display name of the location (e.g., `Canberra Airport`)                                 |
| country_code   | STRING    | The ISO country code for the location (e.g., `US`, `AU`)                                    |
| target_type    | STRING    | The type of location (e.g., `Airport`, `City`, `Borough`, `Canton`, etc.)                   |
| canonical_name | STRING    | The canonical name, often including the location and its administrative hierarchy            |
| tenant         | STRING    | Tenant identifier (for multi-tenant environments)                                            |
| _fivetran_id   | STRING    | Unique identifier for the record (for ETL tracking)                                         |
| _fivetran_synced | TIMESTAMP | Timestamp when the record was last synced (for ETL tracking)                              |

## Example Row

| customer_id | resource_name                | status           | id      | name                    | country_code | target_type | canonical_name                                   | tenant | _fivetran_id | _fivetran_synced |
|-------------|-----------------------------|------------------|---------|-------------------------|--------------|-------------|--------------------------------------------------|--------|--------------|------------------|
| 4592059078  | geoTargetConstants/9041140  | REMOVAL_PLANNED  | 9041140 | Canberra Airport        | AU           | Airport     | Canberra Airport,Australian Capital Territory,AU | ...    | ...          | ...              |

## Usage

- **Join with reporting tables** (e.g., `postal_code_report_campaign_level`) on `resource_name` or `id` to get readable location names and metadata.
- **Filter or group by** location type, country, or status for geographic analysis.
- **Reference for geo targeting** in campaign setup or reporting.

## Notes

- The `status` column indicates whether a geo target constant is currently active (`ENABLED`) or scheduled for removal (`REMOVAL_PLANNED`).
- The `canonical_name` provides a hierarchical description of the location, which can be useful for display or filtering.
- The table is updated regularly to reflect the latest geo target constants from Google Ads. 