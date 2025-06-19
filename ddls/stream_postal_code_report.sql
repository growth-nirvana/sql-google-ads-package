CREATE TABLE `stream_postal_code_report`
(
  customer__id STRING NOT NULL,
  adGroup__id STRING NOT NULL,
  campaign__id STRING NOT NULL,
  geographicView__resourceName STRING,
  geographicView__countryCriterionId STRING NOT NULL,
  geographicView__locationType STRING NOT NULL,
  metrics__clicks FLOAT64,
  metrics__conversions FLOAT64,
  metrics__conversionsValue FLOAT64,
  metrics__costMicros FLOAT64,
  metrics__impressions FLOAT64,
  metrics__allConversions FLOAT64,
  metrics__allConversionsValue FLOAT64,
  segments__geoTargetCity STRING NOT NULL,
  segments__geoTargetPostalCode STRING NOT NULL,
  segments__geoTargetState STRING NOT NULL,
  segments__date STRING NOT NULL,
  run_id INT64,
  tenant STRING,
  _time_extracted TIMESTAMP,
  _time_loaded TIMESTAMP
);