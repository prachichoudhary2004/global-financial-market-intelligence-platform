-- Top Performing Sectors by Rolling 30-Day Return
-- Demonstrates Window Functions and Aggregations

SELECT 
    Sector,
    AVG(Daily_Return) * 30 AS Rolling_30D_Return,
    STDDEV(Daily_Return) AS Sector_Volatility
FROM 
    gold_market_kpis
GROUP BY 
    Sector
ORDER BY 
    Rolling_30D_Return DESC;

-- Portfolio Benchmarking against Market
-- Demonstrates Joins and KPI Calculation

WITH Market_Avg AS (
    SELECT Date, AVG(Daily_Return) as Market_Return
    FROM gold_market_kpis
    GROUP BY Date
)
SELECT 
    p.Symbol,
    p.Date,
    p.Daily_Return,
    m.Market_Return,
    (p.Daily_Return - m.Market_Return) as Alpha
FROM 
    gold_portfolio p
JOIN 
    Market_Avg m ON p.Date = m.Date
WHERE 
    p.Symbol = 'AAPL';

-- Volatility Ranking for Risk Assessment
-- Demonstrates Ranking Functions

SELECT 
    Symbol,
    Volatility_20d,
    RANK() OVER (ORDER BY Volatility_20d DESC) as Risk_Rank
FROM 
    gold_market_kpis
WHERE 
    Date = (SELECT MAX(Date) FROM gold_market_kpis);
