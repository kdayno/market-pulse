CREATE TABLE tabular.dataexpert.kdayno_gold_sp500_stock_prices_avg_agg (
  ticker_symbol STRING,
  company_name STRING,
  trading_date DATE,
  average_trading_price DECIMAL(14,2))
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_sp500_stock_financial_ratios_agg (
  ticker_symbol STRING,
  company_name STRING,
  gics_sector STRING,
  price_to_earnings_ratio DOUBLE,
  return_on_equity_ratio DOUBLE,
  debt_to_equity_ratio DOUBLE,
  market_capitalization DOUBLE,
  sector_average_price_to_earnings_ratio DOUBLE,
  sector_average_return_on_equity_ratio DOUBLE,
  sector_average_debt_to_equity_ratio DOUBLE,
  sector_average_market_capitalization DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_365_days_overall_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_365_days_subreddit_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  subreddit STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_90_days_overall_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_90_days_subreddit_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  subreddit STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_30_days_overall_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;


CREATE TABLE tabular.dataexpert.kdayno_gold_posts_last_30_days_subreddit_sentiment_agg (
  ticker_symbol STRING,
  company_name STRING,
  subreddit STRING,
  sentiment_category STRING,
  sentiment_rank INT,
  post_count BIGINT,
  total_posts BIGINT,
  sentiment_category_percentage DOUBLE,
  average_sentiment_score DOUBLE)
USING delta;