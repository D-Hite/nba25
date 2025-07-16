MODEL (
  name raw.line_team_mapping_table,
   kind SEED (
    path '$root/seeds/line_team_mapping.csv',
  ),
  columns (
    raw_data_team_name VARCHAR,
    log_table_team_name VARCHAR
  )
);