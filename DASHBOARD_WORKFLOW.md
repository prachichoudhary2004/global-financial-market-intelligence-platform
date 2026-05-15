# Global Financial Market Intelligence Platform: Dashboard Workflow

This document serves as a comprehensive guide to understanding and utilizing the features within the platform's dashboard. The platform is designed with an institutional quantitative research aesthetic and leverages a Medallion Data Architecture (Bronze -> Silver -> Gold) alongside AI forecasting and advanced portfolio optimization to provide institutional-grade insights.

## Overview of the Interface
The application is structured into six primary tabs, each serving a distinct analytical purpose. 

At the very top, you will see the **Project Title** with dynamic glowing typography and a scrolling **News/Market Ticker** that mimics a live terminal feed, displaying macro indicators and broad market movements. 

The **Sidebar** houses the **Executive Insight Engine**, an automated qualitative summary that continuously reads the data to alert you of major market shifts, volatility warnings, and sentiment extremes.

---

## 1. Morning Briefing Tab (Executive Landing Page)
**Purpose:** The default landing screen acting as an executive summary for the current trading day.

*   **Live Data Sync & Freshness:** Features a real-time `yfinance` sync button to fetch live market prints, updating the system's "Data Freshness" timestamp and model validation status.
*   **Top Risk Alerts:** Identifies critical volatility spikes and macro risks.
*   **AI Forecast Summary:** Aggregates the strongest high-conviction predictions from the machine learning ensemble.
*   **Sentiment Overview:** Summarizes institutional positioning and trending narratives.

## 2. Market Intel Tab
**Purpose:** Provides a high-level, bird's-eye view of the entire market to gauge breadth, sentiment, and sector-level performance.

*   **Executive Cards:** At the top, you'll see four cards showing the Market Breadth (Gainers vs. Losers ratio), Volatility Regime (a proxy for the VIX), the Dominant Institutional Consensus Regime (e.g., Bullish/Bearish), and the active trading session.
*   **Sector Momentum Heatmap:** A bar chart showing the aggregate 24-hour momentum of different market sectors. It highlights which industries are experiencing capital inflows vs. outflows.
*   **Market Capitalization Heatmap:** A hierarchical treemap grouped by Sector and Symbol. The size of the box represents the company's market cap, while the color represents its daily return (Green = Gain, Red = Loss).
*   **Top Movers Table:** A quick-reference list of the most volatile assets for the current session.

## 3. Stock Terminal Tab
**Purpose:** A deep-dive technical analysis workstation for individual assets.

*   **Asset Search:** Located in the top right corner of the tab. Select any ticker symbol to instantly load its technical data.
*   **Technical Chart:** The main view features an interactive Candlestick chart overlaid with Bollinger Bands (to gauge price channels and mean-reversion) and the 200-day Moving Average (for long-term trend direction).
*   **Momentum Indicators:** Below the main chart are the RSI (Relative Strength Index) and MACD (Moving Average Convergence Divergence) oscillators, helping identify overbought/oversold conditions and short-term trend reversals.

## 4. AI Forecast Tab
**Purpose:** The predictive engine of the platform, utilizing ensemble machine learning to forecast future price movements. **Features on-the-fly lazy loading for all assets.**

*   **Forecast Target:** Located in the top right corner. Selecting an asset will instantly train/load models and trigger inferences.
*   **Prophet Signal:** Uses Facebook's Prophet time-series library to project the expected return over the next 30 days, classifying it as a BUY, SELL, or NEUTRAL signal.
*   **XGBoost Next-Day Prediction:** Uses an XGBoost Regressor trained on technical indicators to predict the closing price for the immediate next trading session.
*   **Model Decision Engine (Explainability):** A horizontal bar chart displaying XGBoost Feature Importance. This visually explains *why* the model made its decision by ranking the impact of technical indicators (like RSI, Volatility, or MA50) for the selected asset.

## 5. Portfolio & Risk Tab
**Purpose:** Enterprise-grade quantitative risk management and optimal asset allocation.

*   **Markowitz Efficient Frontier:** Uses `scipy.optimize` to plot 1,500 simulated portfolios, identifying the exact combination of assets that yields the highest return for the lowest risk. The **Max Sharpe Portfolio** is highlighted with a red star.
*   **Optimal Allocation Table:** Displays the exact percentage weightings you should hold of specific assets to achieve the Maximum Sharpe Ratio.
*   **Monte Carlo Risk Projection:** Simulates 1,000 distinct possible future paths for the optimized portfolio over the next 252 trading days. It calculates the **Mean Projection**, the **5th Percentile Downside Risk (VaR)**, and the exact probability of ending the year at a loss.

## 6. Sentiment Hub Tab
**Purpose:** NLP-driven market sentiment analysis derived from financial news headlines.

*   **Sentiment Momentum Gauge:** A visual dial that indicates the current aggregate sentiment score (ranging from -1.0 Bearish to +1.0 Bullish).
*   **Executive Commentary:** An automated interpretation of the sentiment index, providing a plain-English recommendation on sector positioning.
*   **Institutional Buzzwords:** A simulated word-cloud interface highlighting the most frequent macro themes currently driving market narratives (e.g., "Rate Cuts", "AI Boom").
