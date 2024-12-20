CREATE TABLE IF NOT EXISTS dex_events (
    id SERIAL PRIMARY KEY,
    event_type TEXT,
    block_timestamp BIGINT,
    contract_address TEXT,
    amount0 DOUBLE PRECISION,
    amount1 DOUBLE PRECISION,
    amount0_in DOUBLE PRECISION,
    amount0_out DOUBLE PRECISION,
    amount1_in DOUBLE PRECISION,
    amount1_out DOUBLE PRECISION,
    wallet TEXT
);
CREATE MATERIALIZED VIEW dex_events_sorted AS
SELECT 
    id,
    event_type,
    to_timestamp(block_timestamp) AS readable_date,
    contract_address,
    amount0 / 1e18 as TokenA,
    amount1 / 1e18 as TokenB,
    amount0_in / 1e18 as TokenA_in,
    amount0_out / 1e18 as TokenA_out,
    amount1_in / 1e18 as TokenB_in,
    amount1_out / 1e18 as TokenB_out,
    wallet
FROM dex_events
ORDER BY block_timestamp ASC;

CREATE TABLE smart_contracts (
    address VARCHAR PRIMARY KEY,
    name VARCHAR,
    project VARCHAR,
    projectDapp VARCHAR,
    tags JSONB,
    categories JSONB,
    symbol VARCHAR,
    decimals INTEGER,
    chainId VARCHAR
);
