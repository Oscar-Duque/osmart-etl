UPDATE etl_progress
SET last_raw_ts = :timestamp
WHERE source_name = :source_name