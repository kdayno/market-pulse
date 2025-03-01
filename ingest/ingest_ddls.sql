CREATE TABLE tabular.dataexpert.kdayno_bronze_SP500_companies (
  ticker_symbol STRING,
  company_name STRING,
  gics_sector STRING,
  gics_sub_industry STRING,
  headquarters_location STRING,
  date_added DATE,
  cik INT,
  founded INT,
  load_date_ts TIMESTAMP)
USING delta;

CREATE TABLE tabular.dataexpert.kdayno_bronze_SP500_stock_prices (
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


CREATE TABLE tabular.dataexpert.kdayno_bronze_reddit_top_posts (
  company_name STRING,
  post_id STRING,
  post_title STRING,
  subreddit_id STRING,
  subreddit STRING,
  created_utc DATE,
  score INT,
  upvote_ratio FLOAT,
  num_comments INT,
  post_body_text STRING,
  is_self_post BOOLEAN,
  is_original_content BOOLEAN,
  permalink STRING,
  post_url STRING,
  load_date_ts TIMESTAMP
  )
USING delta
PARTITIONED BY (subreddit)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE tabular.dataexpert.kdayno_bronze_reddit_hot_posts (
  company_name STRING,
  post_id STRING,
  post_title STRING,
  subreddit_id STRING,
  subreddit STRING,
  created_utc DATE,
  score INT,
  upvote_ratio FLOAT,
  num_comments INT,
  post_body_text STRING,
  is_self_post BOOLEAN,
  is_original_content BOOLEAN,
  permalink STRING,
  post_url STRING,
  load_date_ts TIMESTAMP
  )
USING delta
PARTITIONED BY (subreddit)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);