-- Correct option P&L by contract.
--
-- Problem: Robinhood sets Amount=0 for all option buys (BTO, BTC), same as
-- stock buys. The realized_pnl view was treating STC/STO gross proceeds as
-- est_pnl, with nothing deducted for the cost to open — wildly overstating gains.
--
-- Fix: reconstruct open costs from quantity × price × 100 (US equity options
-- are always 100 shares/contract). Cash received (STO, STC) comes from the
-- stored Amount column which is net of Robinhood commissions.
--
-- Per-contract net P&L:
--   cash_received = sum(amount for STO + STC)       -- stored correctly
--   cash_spent    = sum(qty × price × 100 for BTO + BTC) -- reconstructed
--   net_pnl       = cash_received − cash_spent
--
-- is_closed = true when total qty opened == total qty closed for a contract.
-- Use closed_date >= X to find contracts that settled in a given period.

CREATE OR REPLACE VIEW portfolio.option_contract_pnl AS
SELECT
    broker,
    symbol,
    option_expiry,
    option_strike,
    option_type,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))               AS qty_opened,
    sumIf(quantity, trans_code IN ('STC', 'BTC', 'OEXP', 'OASGN')) AS qty_closed,
    round(sumIf(amount,            trans_code IN ('STO', 'STC')), 2) AS cash_received,
    round(sumIf(quantity * price * 100, trans_code IN ('BTO', 'BTC')), 2) AS cash_spent,
    round(
        sumIf(amount,            trans_code IN ('STO', 'STC'))
      - sumIf(quantity * price * 100, trans_code IN ('BTO', 'BTC')),
    2) AS net_pnl,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))
        = sumIf(quantity, trans_code IN ('STC', 'BTC', 'OEXP', 'OASGN')) AS is_closed,
    min(activity_date) AS opened_date,
    max(activity_date) AS closed_date
FROM portfolio.transactions FINAL
WHERE asset_type = 'OPTION'
GROUP BY broker, symbol, option_expiry, option_strike, option_type;
