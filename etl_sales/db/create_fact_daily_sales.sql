CREATE TABLE fact_ventas_diarias (
    fecha DATE NOT NULL,
    tienda_id INT NOT NULL,

    total_ventas DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    numero_ventas INT NOT NULL DEFAULT 0,

    efectivo DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    tarjeta DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    otros DECIMAL(20,2) NOT NULL DEFAULT 0.00,

    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (fecha, tienda_id),

    CONSTRAINT fk_fact_ventas_diarias_tienda
        FOREIGN KEY (tienda_id)
        REFERENCES dim_tienda(tienda_id)
        ON DELETE RESTRICT
        ON UPDATE RESTRICT,

    INDEX idx_fvd_tienda_fecha (tienda_id, fecha)
) ENGINE = InnoDB;