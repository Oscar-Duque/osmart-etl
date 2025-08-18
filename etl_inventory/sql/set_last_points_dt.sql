UPDATE etl_progress
SET last_points_dt = :dt
WHERE store_name = :store_name