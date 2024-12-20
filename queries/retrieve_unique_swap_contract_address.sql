SELECT 
    DISTINCT de.contract_address,  -- Ensure unique contract addresses
    de.event_type,
    sc.name,
    sc.project,
    sc.tags,
    sc.symbol
FROM 
    dex_events de
LEFT JOIN 
    smart_contracts sc
ON 
    de.contract_address = sc.address
WHERE 
    sc.tags::text LIKE '%token%'
AND
    de.event_type LIKE '%SWAP%';
