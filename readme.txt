Para agregar una nueva tienda al sales etl:
- En tabla etl_progress agrebar la nueva tienda con todo null usando:
INSERT INTO etl_progress (store_name)
VALUES ('tienda_nueva');