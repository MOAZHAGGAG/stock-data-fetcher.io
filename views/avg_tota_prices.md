# 📊 Stock Portfolio Analysis Query Breakdown

This SQL query calculates various metrics for stock holdings, leveraging Common Table Expressions (CTEs) and joins to derive insights. Below's a detailed breakdown:

---

## 🧩 CTEs (Common Table Expressions)

### 1. `stock_positions` CTE
**Purpose**: Calculate current holdings and net investment per stock.  
**Columns**:
- `item`: Stock symbol (e.g., `AAPL`).
- `current_shares`: Net shares held (buys add, sells subtract).
- `net_investment`: Net money invested (buys increase, sells decrease).  

**Key Logic**:
- Aggregates buys/sells to compute net shares and investment.
- **Filter**: `HAVING SUM(...) > 0` to exclude stocks with zero/negative shares.

---

### 2. `latest_prices` CTE
**Purpose**: Fetch the latest price for each stock.  
**Columns**:
- `company_code`: Stock symbol.
- `current_price`: Latest available price.
- `trade_date`: Date of the latest price.  

**Logic**:
- Uses a **correlated subquery** to find the maximum `trade_date` per stock.

---

### 3. `portfolio_total` CTE
**Purpose**: Calculate the total portfolio value.  
**Columns**:
- `total_portfolio_value`: Sum of `current_price * current_shares` for all stocks.  

---

## 🔍 Final `SELECT` Statement

### 📋 Columns Calculated
| Column                  | Description                                                                 | Formula                                                                 |
|-------------------------|-----------------------------------------------------------------------------|-------------------------------------------------------------------------|
| `price_as_of_date`      | Latest price date (from `latest_prices`).                                   | `lp.trade_date`                                                        |
| `item`                  | Stock symbol.                                                               | `sp.item`                                                              |
| `current_shares`        | Shares held (from `stock_positions`).                                       | `sp.current_shares`                                                    |
| `avg_price_per_share`   | Average cost per share.                                                     | `COALESCE(civ.avg_cost_per_share, ROUND(net_investment / current_shares, 2))` |
| `current_price`         | Latest stock price.                                                         | `lp.current_price`                                                     |
| `current_value`         | Current market value of holdings.                                           | `ROUND(current_price * current_shares, 2)`                             |
| `total_investment`      | Total money invested.                                                       | `COALESCE(civ.total_investment, ROUND(net_investment, 2))`             |
| `profit_loss_percentage`| Percentage gain/loss.                                                       | `((current_value - total_investment) / total_investment) * 100`        |
| `portfolio_percentage`  | Stock's share of the total portfolio.                                       | `(current_value / total_portfolio_value) * 100`                        |

---

## 🔗 Joins Explained

### 1. `LEFT JOIN investments.avg_share_total_inv_prices`
- **Purpose**: Pull pre-calculated `avg_cost_per_share` and `total_investment`.
- **Use Case**: Avoids redundant calculations if data exists in the `investments` table.

### 2. `JOIN latest_prices`
- **Purpose**: Attach the latest price and trade date to each stock.

### 3. `CROSS JOIN portfolio_total`
- **Purpose**: Adds the total portfolio value to every row for percentage calculations.

---

## ⚙️ Ordering
- Results sorted by stock symbol: `ORDER BY sp.item`.

---

## 🚀 Summary
This query dynamically computes:
- Current holdings, latest prices, and net investment.
- Average cost, profit/loss, and portfolio allocation percentages.
- Uses **pre-calculated values** (if available) for efficiency, otherwise computes on-the-fly.

✅ **Key Takeaway**: Combines real-time data (e.g., `latest_prices`) with historical pre-computed metrics for a comprehensive portfolio analysis.
