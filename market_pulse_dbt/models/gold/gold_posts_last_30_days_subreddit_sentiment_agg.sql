with silver_reddit_all_posts as (
    select 
    * 
    , count(*) over(partition by ticker_symbol, subreddit) as total_posts
    from {{ source('dataexpert', 'kdayno_silver_reddit_all_posts')}}
    where created_date_utc between current_date() - interval 30 day and current_date()
    ),

posts_sentiment_agg as (
    select
        ticker_symbol
        , company_name
        , subreddit
        , sentiment_category
        , count(*) as post_count
        , total_posts
        , round(avg(sentiment_score), 2) as average_sentiment_score
        , round(count(*) / total_posts, 2) as sentiment_category_percentage
    from all_posts
    group by ticker_symbol, company_name, subreddit, sentiment_category, total_posts
    ),        

posts_sentiment_ranked_agg as (
    select
        ticker_symbol
        , company_name
        , subreddit
        , sentiment_category
        , row_number() over(partition by ticker_symbol, subreddit order by sentiment_category_percentage desc) as sentiment_rank
        , post_count
        , total_posts
        , sentiment_category_percentage
        , average_sentiment_score
    from posts_sentiment_agg
    )

select *
from posts_sentiment_ranked_agg
order by ticker_symbol, subreddit, sentiment_rank