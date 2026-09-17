-- ----------------------------
-- Table structure for stock_points
-- ----------------------------
DROP TABLE IF EXISTS `stock_points`;
CREATE TABLE `stock_points`  (
  `source_id` int NOT NULL,
  `art_id` int NOT NULL,
  `point_date` date NOT NULL,
  `sod_stock` bigint NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `tienda_id` int NOT NULL,
  PRIMARY KEY (`source_id`, `art_id`, `point_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf16 COLLATE = utf16_spanish_ci ROW_FORMAT = Dynamic;