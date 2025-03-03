
CREATE OR REPLACE TABLE tabular.dataexpert.kdayno_silver_reddit_top_posts_sentiment (
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
  load_date_ts TIMESTAMP,
  post_title_sentiment_result STRUCT<sentiment: STRING, sentiment_score: FLOAT>,
  sentiment STRING,
  sentiment_score FLOAT)
USING delta
PARTITIONED BY (subreddit)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);


CREATE OR REPLACE TABLE tabular.dataexpert.kdayno_silver_reddit_hot_posts_sentiment (
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
  load_date_ts TIMESTAMP,
  post_title_sentiment_result STRUCT<sentiment: STRING, sentiment_score: FLOAT>,
  sentiment STRING,
  sentiment_score FLOAT)
USING delta
PARTITIONED BY (subreddit)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);


CREATE OR REPLACE TABLE tabular.dataexpert.kdayno_silver_SP500_stock_prices (
  ticker_symbol STRING,
  company_name STRING,
  gics_sector STRING,
  gics_sub_industry STRING,
  open_price DECIMAL(10,2),
  close_price DECIMAL(10,2),
  highest_price DECIMAL(10,2),
  lowest_price DECIMAL(10,2),
  trading_date DATE)
USING delta
PARTITIONED BY (trading_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);


CREATE OR REPLACE TABLE tabular.dataexpert.kdayno_silver_reddit_all_posts (
  ticker_symbol STRING,
  post_id STRING,
  created_date_utc DATE,
  number_of_upvotes INT,
  number_of_downvotes DOUBLE,
  upvote_ratio FLOAT,
  downvote_ratio FLOAT,
  number_of_comments INT,
  sentiment_category STRING,
  sentiment_score FLOAT,
  total_number_of_votes DOUBLE,
  company_name STRING,
  gics_sector STRING,
  gics_sub_industry STRING)
USING delta
PARTITIONED BY (created_date_utc)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

