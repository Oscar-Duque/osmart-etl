#!/bin/bash
cd /home/oscard/osmart-etl
source venv/bin/activate
./venv/bin/python etl_sales/update_clean_data.py
./venv/bin/python etl_inventory/update_raw_stock_movements.py
./venv/bin/python etl_sales/update_stock_points.py