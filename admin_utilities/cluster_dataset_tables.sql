-- cluster_tables
{% assign dataset_id = vars.dataset_id %}
{% assign table_ids = vars.table_ids %}

{% for table_id in table_ids %}
  -- Processing table: {{table_id}}
  -- Step 1: Create new clustered table
  CREATE TABLE `{{dataset_id}}.{{table_id}}_new`
  PARTITION BY date
  CLUSTER BY customer_id
  AS
  SELECT * FROM `{{dataset_id}}.{{table_id}}`;

  -- Step 2: Drop original table
  DROP TABLE `{{dataset_id}}.{{table_id}}`;

  -- Step 3: Create final table with original name
  CREATE TABLE `{{dataset_id}}.{{table_id}}`
  PARTITION BY date
  CLUSTER BY customer_id
  AS
  SELECT * FROM `{{dataset_id}}.{{table_id}}_new`;

  -- Step 4: Drop temporary table
  DROP TABLE `{{dataset_id}}.{{table_id}}_new`;
{% endfor %} 