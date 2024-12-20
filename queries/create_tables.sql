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

CREATE MATERIALIZED VIEW dex_events_normalized AS
SELECT
    id,
    event_type,
    readable_date,
    contract_address,
    wallet,
    -- Consolidate token flows based on the cases
    CASE 
        WHEN COALESCE(amount0, 0) != 0 AND COALESCE(amount1, 0) != 0 THEN 
            CASE WHEN amount0 < 0 THEN COALESCE(amount1, 0) / 1e18 ELSE COALESCE(amount0, 0) / 1e18 END
        WHEN COALESCE(amount0_in, 0) != 0 AND COALESCE(amount1_out, 0) != 0 THEN COALESCE(amount0_in, 0) / 1e18
        WHEN COALESCE(amount0_out, 0) != 0 AND COALESCE(amount1_in, 0) != 0 THEN COALESCE(amount1_in, 0) / 1e18
    END AS token_in,
    CASE 
        WHEN COALESCE(amount0, 0) != 0 AND COALESCE(amount1, 0) != 0 THEN 
            CASE WHEN amount0 < 0 THEN COALESCE(amount0, 0) / 1e18 ELSE COALESCE(amount1, 0) / 1e18 END
        WHEN COALESCE(amount0_in, 0) != 0 AND COALESCE(amount1_out, 0) != 0 THEN COALESCE(amount1_out, 0) / 1e18
        WHEN COALESCE(amount0_out, 0) != 0 AND COALESCE(amount1_in, 0) != 0 THEN COALESCE(amount0_out, 0) / 1e18
    END AS token_out
FROM dex_events_sorted
WHERE 
    (COALESCE(amount0, 0) != 0 AND COALESCE(amount1, 0) != 0) OR
    (COALESCE(amount0_in, 0) != 0 AND COALESCE(amount1_out, 0) != 0) OR
    (COALESCE(amount0_out, 0) != 0 AND COALESCE(amount1_in, 0) != 0);


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
CREATE TABLE contract_info (
            contract_address VARCHAR,
            token0 VARCHAR,
            token1 VARCHAR,
            name_token0 VARCHAR,
            name_token1 VARCHAR
        );

DELETE FROM dex_events 
WHERE contract_address NOT IN (SELECT address FROM smart_contracts);
