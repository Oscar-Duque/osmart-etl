DROP TABLE IF EXISTS stock_points;

-- compact “points” table: one row only when a value changes (or first time seen)
CREATE TABLE stock_points (
  store_id   INT NOT NULL,
  art_id     INT NOT NULL,
  point_date DATE NOT NULL,      -- day the SOD value applies/changed
  sod_stock  BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                     ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (store_id, art_id, point_date)  -- critical for fast lookups
) ENGINE=InnoDB;