DROP TABLE IF EXISTS raw_stock_movements;

CREATE TABLE raw_stock_movements (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    art_id INT NOT NULL,
    tienda_id INT NOT NULL,
    fecha DATETIME NOT NULL,
    tipo_movimiento VARCHAR(30) NOT NULL,
    is_absolute TINYINT(1) NOT NULL DEFAULT 0,
    delta_cantidad BIGINT NULL,
    abs_stock_after BIGINT NULL,
    id_origen VARCHAR(50),
    tabla_origen VARCHAR(30),
    usuario VARCHAR(150),
    extracted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_product_store_date (art_id, tienda_id, fecha),
    INDEX idx_tipo_movimiento (tipo_movimiento),
    INDEX idx_abs (is_absolute, fecha),
    INDEX idx_source_doc (tabla_origen, id_origen)
);