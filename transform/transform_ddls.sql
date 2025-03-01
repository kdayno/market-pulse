
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

