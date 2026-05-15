-- ==============================================================================
-- Script: sector_momentum_ranking.sql
-- Description: Ranks market sectors based on 5-day weighted momentum.
--              Powers the 'Sector Allocation Optimization' UI component.
-- Layer: Gold Analytical View
-- ==============================================================================

WITH AssetMomentum AS (
    SELECT 
        s.Symbol,
        s.Sector,
        s.Market_Cap_B,
        m.Date,
        m.Close,
        -- 5-Day Rate of Change (Momentum)
        ((m.Close - LAG(m.Close, 5) OVER(PARTITION BY m.Symbol ORDER BY m.Date)) / 
         NULLIF(LAG(m.Close, 5) OVER(PARTITION BY m.Symbol ORDER BY m.Date), 0)) * 100 AS Momentum_5d
    FROM silver.market_data m
    JOIN bronze.company_metadata s ON m.Symbol = s.Symbol
    WHERE m.Date = (SELECT MAX(Date) FROM silver.market_data) -- Latest Trading Day
),
SectorAggregate AS (
    SELECT 
        Sector,
        -- Market Cap Weighted Momentum
        SUM(Momentum_5d * Market_Cap_B) / NULLIF(SUM(Market_Cap_B), 0) AS Weighted_Sector_Momentum,
        SUM(Market_Cap_B) AS Total_Sector_Cap_B,
        COUNT(DISTINCT Symbol) AS Asset_Count
    FROM AssetMomentum
    WHERE Momentum_5d IS NOT NULL
    GROUP BY Sector
)
SELECT 
    Sector,
    Weighted_Sector_Momentum,
    Total_Sector_Cap_B,
    Asset_Count,
    -- Rank sectors by strongest positive momentum
    RANK() OVER(ORDER BY Weighted_Sector_Momentum DESC) AS Momentum_Rank,
    CASE 
        WHEN Weighted_Sector_Momentum > 2.0 THEN 'Strong Capital Inflow'
        WHEN Weighted_Sector_Momentum < -2.0 THEN 'Severe Capital Outflow'
        ELSE 'Neutral Flow'
    END AS Flow_Regime
FROM SectorAggregate
ORDER BY Momentum_Rank ASC;
