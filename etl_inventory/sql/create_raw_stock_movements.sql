DROP TABLE IF EXISTS `raw_stock_movements`;

CREATE TABLE `raw_stock_movements`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `tienda_id` int NOT NULL,
  `source_id` int NOT NULL,
  `art_id` int NOT NULL,

  `his_id` INT NOT NULL,
  `id_origen` int NOT NULL,
  `tabla_origen` varchar(30) CHARACTER SET utf16 COLLATE utf16_spanish_ci NULL DEFAULT NULL,

  `fecha` datetime NOT NULL,
  `tipo_movimiento` varchar(30) CHARACTER SET utf16 COLLATE utf16_spanish_ci NOT NULL,
  `is_absolute` tinyint(1) NOT NULL DEFAULT 0,
  `delta_cantidad` bigint NULL DEFAULT NULL,
  `abs_stock_after` bigint NULL DEFAULT NULL,

  `usuario` varchar(150) CHARACTER SET utf16 COLLATE utf16_spanish_ci NULL DEFAULT NULL,
  `extracted_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`id`),
  UNIQUE INDEX `uq_hist_event` (`source_id`, `his_id`, `art_id`),

  INDEX `idx_product_store_date`(`art_id`, `source_id`, `fecha`),
  INDEX `idx_tipo_movimiento`(`tipo_movimiento`),
  INDEX `idx_abs`(`is_absolute`, `fecha`),
  INDEX `idx_source_doc`(`tabla_origen`, `id_origen`)
) ENGINE = InnoDB
CHARACTER SET = utf16 
COLLATE = utf16_spanish_ci;