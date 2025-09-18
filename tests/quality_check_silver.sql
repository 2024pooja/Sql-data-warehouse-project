/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


/*from the table broze.crm_cust_info*/

-- check for null or duplicate in primary key 
-- exception: no result 

select cst_id,count(*) from broze.crm_cust_info group by cst_id having count(*) >1 or cst_id is null;

-- check for unwanted spaces 
-- exception: no result 
/*if the  original value is not equal to the same value after trimming it means there are spaces */

select cst_firstname from broze.crm_cust_info where cst_firstname != trim(cst_firstname);
select cst_lastname from broze.crm_cust_info where cst_lastname != trim(cst_lastname);
/*similarly, we can check for the whole string value column */

select cst_gndr from broze.crm_cust_info where cst_gndr != trim(cst_gndr);/*No value thus the quality of gender is bettre*/

-- data standardization & consistency 

 /*in our DWH we aim to store clear and meaningful values rather than using abbreviated terms */
 /*in our DWH we use the default value 'n/a' for missing values */
 select distinct cst_gndr from broze.crm_cust_info;
 select distinct cst_material_status from broze.crm_cust_info;
 
 
 
 /*from the table broze.crm_prd_info*/
 
 -- check for null or duplicate in primary key 
-- exception: no result 

select prd_id,count(*) from broze.crm_prd_info group by prd_id having count(*) >1 or prd_id is null;
-- check for unwanted spaces 
-- exception: no result 
/*if the  original value is not equal to the same value after trimming it means there are spaces */

select prd_nm from broze.crm_prd_info where prd_nm!= trim(prd_nm);

/*Null or negative number in cost*/
select prd_cost from broze.crm_prd_info where  prd_cost < 0 or prd_cost =' ';
/* data standardization & consistencty*/
select distinct prd_line from broze.crm_prd_info ;

/* check for valid date orders*/

-- end date must be earlier than the start date

select * from broze.crm_prd_info where prd_start_dt > prd_end_dt;

/* end date = start date of the next record - 1*/

/*  qulity check for crm_sales_details*/

select sls_ord_num from broze.crm_sales_details where sls_ord_num != trim(sls_ord_num); -- no result so it is fine 
/* check for sls_prd_key , sls_cust_id*/
select sls_prd_key from broze.crm_sales_details where sls_prd_key != trim(sls_prd_key) ;
select sls_cust_id from broze.crm_sales_details where sls_cust_id != trim(sls_cust_id) ;
select prd_key , count(*) from silver.crm_prd_info group by prd_key having prd_key not in (  select sls_prd_key from broze.crm_sales_details   );

 select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from broze.crm_sales_details
 where sls_cust_id not in ( select cst_id from silver.crm_cust_info );


  select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from broze.crm_sales_details
 where sls_prd_key not in ( select prd_key from silver.crm_prd_info );
 
SELECT s.sls_ord_num,
       s.sls_prd_key,
       s.sls_cust_id,
       s.sls_order_dt,
       s.sls_ship_dt,
       s.sls_due_dt,
       s.sls_sales,
       s.sls_quantity,
       s.sls_price
FROM broze.crm_sales_details s
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_prd_info p
    WHERE p.prd_key = s.sls_prd_key
);


 
 select distinct TRIM(sls_prd_key) as sales_key
from broze.crm_sales_details s
where not exists (
    select 1 from silver.crm_prd_info p
    where TRIM(s.sls_prd_key) = TRIM(p.prd_key)
);


  select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from broze.crm_sales_details
 where sls_prd_key not in ( select prd_key from silver.crm_prd_info );

 select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from broze.crm_sales_details
 where sls_prd_key not in ( select trim(prd_key) from silver.crm_prd_info );

select prd_key from silver.crm_prd_info where prd_key!= trim(prd_key);

/* dates are in the form integer, so we have to convert that into a date */

-- Any negative number or zero can't be converted into a  date 
select sls_order_dt from broze.crm_sales_details where sls_order_dt <=0;
/* there are lots of zeros, we will replace them with null */

SELECT 
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM broze.crm_sales_details
WHERE sls_order_dt <= 0 or length(sls_order_dt) !=8 or sls_order_dt > 20500101;


SELECT 
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM broze.crm_sales_details
WHERE sls_ship_dt <= 0 or length(sls_ship_dt) !=8 or sls_ship_dt > 20500101;

SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM broze.crm_sales_details
WHERE sls_due_dt <= 0 or length(sls_due_dt) !=8 or sls_due_dt > 20500101;

select sls_order_dt,sls_ship_dt,sls_due_dt from broze.crm_sales_details where sls_order_dt < sls_ship_dt or  sls_order_dt <sls_due_dt;

-- length is less than 8 or greater than 8 we have an issue 
-- also the range of the date 


-- business Rules 
-- sales = quantity * price 
-- negative, zero, nulls are not allowed 

select sls_sales,sls_quantity,sls_price
from broze.crm_sales_details
WHERE sls_sales!= sls_quantity*sls_price
or sls_sales is NULL or sls_quantity is NULL or sls_price is NULL
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0;

SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM broze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_sales = '' OR sls_sales <= 0
   OR sls_quantity IS NULL OR sls_quantity = '' OR sls_quantity <= 0
   OR sls_price IS NULL OR sls_price = '' OR sls_price <= 0 ORDER BY  sls_sales,
    sls_quantity,
    sls_price;
    
    
    -- IF SALES IS NEGATIVE, ZERO, NULL, DERIVE IT USING Q*prepare
    -- IF PRICE IS ZERO, NULL, DERIVE IT USING S/Q
    -- IF PRICE IS NEGATIVE, CONVERT IT INTO POSITIVE
    
    SELECT sls_sales AS OLD_sls_sales ,sls_quantity AS OLD_sls_quantity ,sls_price AS OLD_sls_price,
    CASE WHEN sls_sales = NULL OR sls_sales = ''or sls_sales<=0 or sls_sales!=sls_sales * sls_quantity THEN  ABS(sls_price)* sls_quantity 
    else sls_sales end as sls_sales,
	CASE WHEN sls_price = NULL OR sls_price = ''or sls_price <=0 or sls_price!=(sls_sales )/nullif( sls_quantity,0) THEN  ROUND(ABS(sls_sales) /nullif( sls_quantity,0) ,0)
    else sls_sales end as sls_price 
    FROM broze.crm_sales_details order by old_sls_sales,
    old_sls_quantity,
    old_sls_price;
    

SELECT COUNT(DISTINCT sls_prd_key) FROM broze.crm_sales_details;
SELECT COUNT(DISTINCT prd_key) FROM silver.crm_prd_info;

SELECT DISTINCT sls_prd_key
FROM broze.crm_sales_details
WHERE TRIM(LOWER(sls_prd_key)) NOT IN (
    SELECT TRIM(LOWER(prd_key)) 
    FROM silver.crm_prd_info
);

SELECT DISTINCT sls_prd_key, 
       LENGTH(sls_prd_key) AS len_sales
FROM broze.crm_sales_details
WHERE sls_prd_key LIKE '% %' OR sls_prd_key LIKE CHAR(9) OR sls_prd_key LIKE CHAR(13);

SELECT DISTINCT s.sls_prd_key, p.prd_key
FROM broze.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
    ON TRIM(LOWER(s.sls_prd_key)) = TRIM(LOWER(p.prd_key))
WHERE p.prd_key IS NULL;



/* erp_cust_az12 */
select cid , bdate,gen from broze.erp_cust_az12; /* extra charater which are not in cust_key in broze.crm_cust_info */
select * from broze.crm_cust_info;

select cid, case
when cid like'NAS%' THEN substring(cid, 4, length(cid)) 
ELSE cid
end as new_cid ,
bdate,gen
from broze.erp_cust_az12; 

select cid, case
when cid like'NAS%' THEN substring(cid, 4, length(cid)) 
ELSE cid
end as new_cid ,
bdate,gen
from broze.erp_cust_az12
WHERE (
       CASE
         WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
         ELSE cid
       END
     ) NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
     
     -- checking the range of bday
     select distinct bdate from broze.erp_cust_az12 where bdate <'1924-01-01' or bdate > now(); -- bdate are in furture 
     
select  case
when cid like'NAS%' THEN substring(cid, 4, length(cid)) 
ELSE cid
end as new_cid ,
case when bdate > now() then NULL 
ELSE bdate 
end as bdate
,gen
from broze.erp_cust_az12;

select distinct gen
from broze.erp_cust_az12; /* blank, null, M, F, Male, Female*/
select
case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
ELSE 'n/a'
end as gen from broze.erp_cust_az12;

/* erp_loc_a101*/

select cid,cntry from broze.erp_loc_a101;  /* use of specila charater in between cid */
select cst_key from silver.crm_cust_info;

select replace(cid,'-','') as cid from broze.erp_loc_a101;

select replace(cid,'-','') as cid from broze.erp_loc_a101 where replace(cid,'-','') not in  ( select cst_key from silver.crm_cust_info );

select distinct cntry from  broze.erp_loc_a101;

select case 
when trim(cntry) = 'DE' THEN 'Germany'
when trim(cntry) in ('US','USA') THEN 'United States'
when trim(cntry) = '' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
 from  broze.erp_loc_a101;
 
 /* erp_px_cat_g1v2*/
 
 select id,cat,subcat,maintenance from broze.erp_px_cat_g1v2; -- id is equivalent to cat_id 
select cat_id from silver.crm_prd_info;


select distinct cat from broze.erp_px_cat_g1v2; 
select distinct subcat from broze.erp_px_cat_g1v2; 
select distinct maintenance from broze.erp_px_cat_g1v2; 

select cat , subcat , maintenance from broze.erp_px_cat_g1v2 
where trim(cat)!= cat or trim(subcat)!= subcat or trim(maintenance)!= maintenance;


/* after data cleaning, again checking the value in the silver layer*/

/* Now checking the data quality in silver DB*/

-- check for null or duplicate in primary key 
-- exception: no result 

select cst_id,count(*) from silver.crm_cust_info group by cst_id having count(*) >1 or cst_id is null;

-- check for unwanted spaces 
-- exception: no result 
/*if the  original value is not equal to the same value after trimming it means there are spaces */

select cst_firstname from silver.crm_cust_info where cst_firstname != trim(cst_firstname);
select cst_lastname from silver.crm_cust_info where cst_lastname != trim(cst_lastname);
/*similarly, we can check for the whole string value column */

select cst_gndr from silver.crm_cust_info where cst_gndr != trim(cst_gndr);/*No value thus the quality of gender is bettre*/
select distinct cst_gndr from silver.crm_cust_info;

-- data standardization & consistency 

 /*in our DWH we aim to store clear and meaningful values rather than using abbreviated terms */
 /*in our DWH we use the default value 'n/a' for missing values */
 select distinct cst_gndr from silver.crm_cust_info;
 select distinct cst_material_status from silver.crm_cust_info;
 
 
  select * from silver.crm_cust_info;
 
  /*from the table broze.crm_prd_info*/
 
 -- check for null or duplicate in primary key 
-- exception: no result 

select prd_id,count(*) from silver.crm_prd_info group by prd_id having count(*) >1 or prd_id is null;
-- check for unwanted spaces 
-- exception: no result 
/*if the  original value is not equal to the same value after trimming it means there are spaces */

select prd_nm from silver.crm_prd_info where prd_nm!= trim(prd_nm);

/*Null or negative number in cost*/
select prd_cost from silver.crm_prd_info where  prd_cost < 0 or prd_cost ='';
/* data standardization & consistencty*/
select distinct prd_line from silver.crm_prd_info ;

/* check for valid date orders*/

-- end date must be earlier than the start date

select * from silver.crm_prd_info where prd_start_dt > prd_end_dt;

/* end date = start date of the next record - 1*/

select * from silver.crm_prd_info;


/*  qulity check for crm_sales_details*/

select sls_ord_num from silver.crm_sales_details where sls_ord_num != trim(sls_ord_num); -- no result so it is fine 
/* check for sls_prd_key , sls_cust_id*/
select sls_prd_key from silver.crm_sales_details where sls_prd_key != trim(sls_prd_key) ;
select sls_cust_id from silver.crm_sales_details where sls_cust_id != trim(sls_cust_id) ;
select prd_key , count(*) from silver.crm_prd_info group by prd_key having prd_key not in (  select sls_prd_key from broze.crm_sales_details   );

 select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from silver.crm_sales_details
 where sls_cust_id not in ( select cst_id from silver.crm_cust_info );
 
  select sls_ord_num,trim(sls_prd_key),sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from silver.crm_sales_details
 where trim(sls_prd_key) not in ( select prd_key from silver.crm_prd_info );

  select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from silver.crm_sales_details
 where sls_prd_key not in ( select prd_key from silver.crm_prd_info );

 select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price from silver.crm_sales_details
 where sls_prd_key not in ( select trim(prd_key) from silver.crm_prd_info );

select prd_key from silver.crm_prd_info where prd_key!= trim(prd_key);

/* dates are in the form integer, so we have to convert that into a date */

-- Any negative number or zero can't be converted into date 
select sls_order_dt from silver.crm_sales_details where sls_order_dt <=0;
/* there are lots of zero we will replace them with null */

SELECT 
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 or length(sls_order_dt) !=8 or sls_order_dt < 20500101;


SELECT 
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM broze.crm_sales_details
WHERE sls_ship_dt <= 0 or length(sls_ship_dt) !=8 or sls_ship_dt > 20500101;

SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 or length(sls_due_dt) !=8 or sls_due_dt > 20500101 order by sls_due_dt;

select sls_order_dt,sls_ship_dt,sls_due_dt from silver.crm_sales_details where sls_order_dt > sls_ship_dt or  sls_order_dt > sls_due_dt;

-- length is less than 8 or greater than 8 we have an issue 
-- also the range of the date 


-- business Rules 
-- sales = quantity * price 
-- negative , zero , nulls are not allowed 

select sls_sales,sls_quantity,sls_price
from silver.crm_sales_details
WHERE sls_sales!= sls_quantity*sls_price
or sls_sales is NULL or sls_quantity is NULL or sls_price is NULL
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0;

SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details

WHERE sls_sales != sls_quantity * sls_price
 ORDER BY  sls_sales,
    sls_quantity,
    sls_price;
    
    
    -- IF SALES IS NEGATIVE, ZERO, NULL, DERIVE IT USING Q*prepare
    -- IF PRICE IS ZERO, NULL, DERIVE IT USING S/Q
    -- IF PRICE IS NEGATIVE, CONVERT IT INTO POSITIVE
    
    SELECT sls_sales AS OLD_sls_sales ,sls_quantity AS OLD_sls_quantity ,sls_price AS OLD_sls_price,
    CASE WHEN sls_sales = NULL OR sls_sales = ''or sls_sales<=0 or sls_sales!=sls_sales * sls_quantity THEN  ABS(sls_price)* sls_quantity 
    else sls_sales end as sls_sales,
	CASE WHEN sls_price = NULL OR sls_price = ''or sls_price <=0 or sls_price!=(sls_sales )/nullif( sls_quantity,0) THEN  ROUND(ABS(sls_sales) /nullif( sls_quantity,0) ,0)
    else sls_sales end as sls_price 
    FROM broze.crm_sales_details order by old_sls_sales,
    old_sls_quantity,
    old_sls_price;
    
select * from silver.crm_sales_details where sls_sales!=sls_price*sls_quantity;
SELECT * 
FROM silver.crm_sales_details 
WHERE sls_sales != sls_price * sls_quantity
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;


/* checking for qulity */
/* erp_cust_az12 */
select cid  from silver.erp_cust_az12 /* extra charater which are not in cust_key in broze.crm_cust_info */
where cid not in (select cst_key from broze.crm_cust_info);

select distinct bdate from silver.erp_cust_az12 where /*bdate <'1924-01-01' or*/ bdate > now(); -- bdate are in furture 
select distinct gen
from silver.erp_cust_az12; /* blank, null, M, F, Male,F emale*/


/**/

select distinct cntry from  silver.erp_loc_a101;

 select id,cat,subcat,maintenance from silver.erp_px_cat_g1v2; -- id is equvalant to cat_id 
select cat_id from silver.crm_prd_info;


select distinct cat from silver.erp_px_cat_g1v2; 
select distinct subcat from silver.erp_px_cat_g1v2; 
select distinct maintenance from silver.erp_px_cat_g1v2; 

select cat , subcat , maintenance from silver.erp_px_cat_g1v2
where trim(cat)!= cat or trim(subcat)!= subcat or trim(maintenance)!= maintenance;
























 
 
 
 
 
 
 
 
 
 
