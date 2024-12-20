SELECT 
    event_type, 
    COUNT(*) AS event_count
FROM dex_events_sorted
GROUP BY event_type
ORDER BY event_count DESC;
