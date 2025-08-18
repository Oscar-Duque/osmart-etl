UPDATE etl_progress
SET last_raw_ts = :ts
WHERE store_name = :store_name