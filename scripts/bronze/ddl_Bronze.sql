/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    If they already exist.
	  Run this script to redefine the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE TABLE IF NOT EXISTS broze.crm_cust_info (
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_material_status varchar(50),
cst_gndr varchar(50),
cst_create_date date

);

SHOW VARIABLES LIKE 'secure_file_priv';
drop table broze.crm_cust_info;

CREATE TABLE IF NOT EXISTS broze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);

CREATE TABLE IF NOT EXISTS broze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int

);


CREATE TABLE IF NOT EXISTS broze.erp_loc_a101(
cid varchar(50),
cntry varchar(50)
);

CREATE TABLE IF NOT EXISTS broze.erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50)
);

CREATE TABLE IF NOT EXISTS broze.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50)
);

select * from broze.crm_cust_info;
select count(*) from broze.crm_cust_info;

select * from broze.crm_prd_info;
select count(*) from broze.crm_prd_info;

select * from broze.crm_sales_details;
select count(*) from broze.crm_sales_details;
select sls_prd_key, count(*) from broze.crm_sales_details group by sls_prd_key ;

select * from broze.erp_loc_a101;
select count(*) from broze.erp_loc_a101;

select * from broze.erp_cust_az12;
select count(*) from broze.erp_cust_az12;

select * from broze.erp_px_cat_g1v2;
select count(*) from broze.erp_px_cat_g1v2;



-- table is loaded, using import wizard




