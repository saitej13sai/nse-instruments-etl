CREATE TABLE IF NOT EXISTS dhan_nse (
    exchange TEXT,
    security_id TEXT PRIMARY KEY,
    symbol_name TEXT,
    trading_symbol TEXT UNIQUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trading_symbol ON dhan_nse (trading_symbol);