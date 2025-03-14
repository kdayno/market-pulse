with silver_SP500_stock_prices as (
    select *
    from {{ source('dataexpert', 'kdayno_silver_SP500_stock_prices')}}
    ),

    gold_sp500_stock_prices_avg_agg as (
    select 
        ticker_symbol
        , company_name
        , trading_date
        , round(avg((open_price + close_price + highest_price + lowest_price)) / 4, 2) AS average_trading_price
    from silver_SP500_stock_prices
    group by ticker_symbol, company_name, trading_date
    )

    select * from gold_sp500_stock_prices_avg_agg