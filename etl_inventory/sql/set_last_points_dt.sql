UPDATE etl_progress
SET last_points_dt = :dt
WHERE source_name = :source_name