WITH target AS (SELECT COALESCE(:as_of_date, MAX(point_date)) AS as_of_date FROM stock_points WHERE source_id = :source_id),
ranked AS (
    SELECT
        sp.art_id,
        sp.point_date,
        sp.sod_stock,
        ROW_NUMBER() OVER (PARTITION BY sp.art_id ORDER BY sp.point_date DESC, sp.updated_at DESC) AS rn
    FROM
        stock_points sp
        JOIN target t ON sp.point_date <= t.as_of_date
    WHERE
        sp.source_id = :source_id
) SELECT
    art_id,
    sod_stock
FROM
    ranked
WHERE
    rn = 1
ORDER BY
    art_id;