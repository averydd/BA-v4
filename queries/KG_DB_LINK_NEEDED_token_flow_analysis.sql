SELECT 
    contract_address,
    SUM(
        CASE 
            WHEN COALESCE(TokenA, 0) != 0 AND COALESCE(TokenB, 0) != 0 THEN ABS(COALESCE(TokenA, 0))
            WHEN COALESCE(TokenA_in, 0) != 0 AND COALESCE(TokenB_out, 0) != 0 THEN ABS(COALESCE(TokenA_in, 0))
            WHEN COALESCE(TokenA_out, 0) != 0 AND COALESCE(TokenB_in, 0) != 0 THEN ABS(COALESCE(TokenA_out, 0))
            ELSE 0
        END
    ) AS total_TokenA_traded,
    SUM(
        CASE 
            WHEN COALESCE(TokenA, 0) != 0 AND COALESCE(TokenB, 0) != 0 THEN ABS(COALESCE(TokenB, 0))
            WHEN COALESCE(TokenA_in, 0) != 0 AND COALESCE(TokenB_out, 0) != 0 THEN ABS(COALESCE(TokenB_out, 0))
            WHEN COALESCE(TokenA_out, 0) != 0 AND COALESCE(TokenB_in, 0) != 0 THEN ABS(COALESCE(TokenB_in, 0))
            ELSE 0
        END
    ) AS total_TokenB_traded
FROM dex_events_sorted
WHERE event_type = 'SWAP'
GROUP BY contract_address
ORDER BY 
    SUM(
        CASE 
            WHEN COALESCE(TokenA, 0) != 0 AND COALESCE(TokenB, 0) != 0 THEN ABS(COALESCE(TokenA, 0))
            WHEN COALESCE(TokenA_in, 0) != 0 AND COALESCE(TokenB_out, 0) != 0 THEN ABS(COALESCE(TokenA_in, 0))
            WHEN COALESCE(TokenA_out, 0) != 0 AND COALESCE(TokenB_in, 0) != 0 THEN ABS(COALESCE(TokenA_out, 0))
            ELSE 0
        END
    ) + 
    SUM(
        CASE 
            WHEN COALESCE(TokenA, 0) != 0 AND COALESCE(TokenB, 0) != 0 THEN ABS(COALESCE(TokenB, 0))
            WHEN COALESCE(TokenA_in, 0) != 0 AND COALESCE(TokenB_out, 0) != 0 THEN ABS(COALESCE(TokenB_out, 0))
            WHEN COALESCE(TokenA_out, 0) != 0 AND COALESCE(TokenB_in, 0) != 0 THEN ABS(COALESCE(TokenB_in, 0))
            ELSE 0
        END
    ) DESC;
