-- ==============================================================================
-- Script: anomaly_detection.sql
-- Description: Identifies 3-sigma standard deviation price/volume anomalies.
--              Flags 'Black Swan' type events or flash crashes.
-- Layer: Gold Analytical View
-- ==============================================================================

WITH RollingStats AS (
    SELECT 
        Symbol,
        Date,
        Close,
        Volume,
        -- 20-Day Moving Averages
        AVG(Close) OVER w_20 AS MA_20d,
        AVG(Volume) OVER w_20 AS AvgVol_20d,
        -- 20-Day Standard Deviations
        STDDEV(Close) OVER w_20 AS StdDev_Close_20d,
        STDDEV(Volume) OVER w_20 AS StdDev_Vol_20d
    FROM silver.market_data
    WINDOW w_20 AS (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
),
ZScores AS (
    SELECT 
        Symbol,
        Date,
        Close,
        Volume,
        MA_20d,
        -- Calculate Z-Scores
        (Close - MA_20d) / NULLIF(StdDev_Close_20d, 0) AS Price_ZScore,
        (Volume - AvgVol_20d) / NULLIF(StdDev_Vol_20d, 0) AS Volume_ZScore
    FROM RollingStats
)
SELECT 
    Symbol,
    Date,
    Close,
    Price_ZScore,
    Volume_ZScore,
    CASE 
        WHEN Price_ZScore <= -3.0 AND Volume_ZScore >= 2.0 THEN 'Flash Crash / Capitulation'
        WHEN Price_ZScore >= 3.0 AND Volume_ZScore >= 2.0 THEN 'Euphoric Blow-off Top'
        WHEN Volume_ZScore >= 4.0 THEN 'Extreme Institutional Volume'
        ELSE 'Normal Variance'
    END AS Anomaly_Classification
FROM ZScores
WHERE ABS(Price_ZScore) >= 3.0 OR Volume_ZScore >= 4.0
ORDER BY Date DESC, ABS(Price_ZScore) DESC;
