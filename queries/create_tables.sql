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

CREATE MATERIALIZED VIEW dex_events_normalized AS
SELECT
    id,
    event_type,
    block_timestamp,
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
FROM dex_events
WHERE 
    (
        (COALESCE(amount0, 0) != 0 AND COALESCE(amount1, 0) != 0) OR
        (COALESCE(amount0_in, 0) != 0 AND COALESCE(amount1_out, 0) != 0) OR
        (COALESCE(amount0_out, 0) != 0 AND COALESCE(amount1_in, 0) != 0)
    )
    AND event_type = 'SWAP';

CREATE TABLE contract_info (
            contract_address VARCHAR,
            token0_address VARCHAR,
            token1_address VARCHAR,
            token0_symbol VARCHAR,
            token1_symbol VARCHAR
        );

CREATE TABLE token_prices (
            token_address VARCHAR,
            price DOUBLE PRECISION,
            block_timestamp BIGINT
        );

