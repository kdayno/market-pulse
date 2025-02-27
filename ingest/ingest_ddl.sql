CREATE TABLE tabular.dataexpert.kdayno_ingest_SP500Companies (
  Symbol STRING,
  Security STRING,
  GICS_Sector STRING,
  GICS_Sub_Industry STRING,
  Headquarters_Location STRING,
  Date_added DATE,
  CIK INT,
  Founded INT,
  load_date_ts TIMESTAMP)
USING delta;

CREATE TABLE tabular.dataexpert.kdayno_ingest_SP500_stock_prices (
  ticker_symbol STRING,
  open_price DECIMAL(10,2),
  close_price DECIMAL(10,2),
  highest_price DECIMAL(10,2),
  lowest_price DECIMAL(10,2),
  trading_date DATE,
  load_date_ts TIMESTAMP)
USING delta
PARTITIONED BY (trading_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
