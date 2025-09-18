/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_material_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP


);

SHOW VARIABLES LIKE 'secure_file_priv';
drop table silver.crm_cust_info;

CREATE TABLE IF NOT EXISTS silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt DATE,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP

);

drop table silver.crm_prd_info;

CREATE TABLE IF NOT EXISTS silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP


);
drop table silver.crm_sales_details;

CREATE TABLE IF NOT EXISTS silver.erp_loc_a101(
cid varchar(50),
cntry varchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP

);

drop table silver.erp_loc_a101;

CREATE TABLE IF NOT EXISTS silver.erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP

);

select * from silver.crm_cust_info;
select count(*) from silver.crm_cust_info;

select * from silver.crm_prd_info;
select count(*) from silver.crm_prd_info;
select prd_key , count(*) from silver.crm_prd_info group by prd_key;

select sls_prd_key , count(*) from silver.crm_sales_details group by sls_prd_key;
select count(*) from silver.crm_sales_details;

select * from silver.erp_loc_a101;
select count(*) from silver.erp_loc_a101;

select * from silver.erp_cust_az12;
select count(*) from silver.erp_cust_az12;

select * from silver.erp_px_cat_g1v2;
select count(*) from silver.erp_px_cat_g1v2;






