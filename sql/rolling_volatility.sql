-- ==============================================================================
-- Script: rolling_volatility.sql
-- Description: Calculates 30-day rolling volatility and annualized risk metrics.
--              Used by the Portfolio Optimization Engine for covariance matrices.
-- Layer: Gold Analytical View
-- ==============================================================================

WITH DailyReturns AS (
    SELECT 
        Symbol,
        Date,
        Close,
        (Close - LAG(Close, 1) OVER(PARTITION BY Symbol ORDER BY Date)) / 
         NULLIF(LAG(Close, 1) OVER(PARTITION BY Symbol ORDER BY Date), 0) AS Daily_Return
    FROM silver.market_data
)
SELECT 
    Symbol,
    Date,
    Close,
    Daily_Return,
    -- 30-day Rolling Standard Deviation (Volatility)
    STDDEV(Daily_Return) OVER(
        PARTITION BY Symbol 
        ORDER BY Date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS Volatility_30d,
    
    -- Annualized Volatility (assuming 252 trading days)
    STDDEV(Daily_Return) OVER(
        PARTITION BY Symbol 
        ORDER BY Date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * SQRT(252) AS Annualized_Volatility
FROM DailyReturns
WHERE Daily_Return IS NOT NULL
ORDER BY Symbol, Date DESC;
