SELECT last_raw_ts 
FROM etl_progress 
WHERE store_name = :store_name;