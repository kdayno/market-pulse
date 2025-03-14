{{ config(materialized=set_materialization()) }}

with silver_SP500_stock_financials as (
    select *
    from {{ source('dataexpert', 'kdayno_silver_SP500_stock_financials')}}
    ),

    gold_SP500_stock_financial_ratios_agg as (
    select 
        ticker_symbol
        , company_name
        , gics_sector
        , coalesce(price_to_earnings_ratio, 0) as price_to_earnings_ratio
        , coalesce(return_on_equity_ratio, 0) as return_on_equity_ratio
        , coalesce(debt_to_equity_ratio, 0) as debt_to_equity_ratio
        , coalesce(market_capitalization, 0) as market_capitalization
        , avg(price_to_earnings_ratio) over(partition by gics_sector) as sector_average_price_to_earnings_ratio
        , avg(return_on_equity_ratio) over(partition by gics_sector) as sector_average_return_on_equity_ratio
        , avg(debt_to_equity_ratio) over(partition by gics_sector) as sector_average_debt_to_equity_ratio
        , avg(market_capitalization) over(partition by gics_sector) as sector_average_market_capitalization
    from silver_SP500_stock_financials
    )

    select * from gold_SP500_stock_financial_ratios_agg