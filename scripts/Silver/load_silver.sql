 CREATE OR ALTER PROCEDURE silver.load_silver AS 
 BEGIN

        TRUNCATE TABLE Silver.crm_cust_info; 
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date)

        SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN cst_material_status = 'S' THEN 'SINGLE'
            WHEN cst_material_status = 'M' THEN 'MARRIED'
            ELSE 'n/a'
        END cst_material_status,
        CASE WHEN cst_gndr = 'F' THEN 'FEMALE'
            WHEN cst_gndr = 'M' THEN 'MALE'
            ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        FROM(
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY  cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        )t WHERE flag_last = 1 

        TRUNCATE TABLE Silver.crm_prd_info 
        INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_lcost,
        prd_line,
        prd_start_dt,
        prd_end_dt
        )
        SELECT
        prd_id,
        REPlACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
        SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_lcost,0) AS prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info



        TRUNCATE TABLE Silver.crm_sales_details 
        INSERT INTO silver.crm_sales_details(
            sls_ord_num
            ,sls_prd_key
            ,sls_cust_id
            ,sls_order_dt
            ,sls_ship_dt
            ,sls_due_dt
            ,sls_sales
            ,sls_quantity
            ,sls_price
        )


        SELECT  sls_ord_num
            ,sls_prd_key
            ,sls_cust_id,
            Case When sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                END AS sls_order_dt,
            Case When sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                END AS sls_ship_dt,
            Case When sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                END AS sls_due_dt
            ,CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                    END AS sls_sales
            ,sls_quantity
            ,CASE WHEN sls_price IS NULL OR sls_price <=0
                    THEN sls_sales / NULLIF(sls_quantity,0)
                    ELSE sls_price
                END AS sls_price
        FROM Datawarehouse.bronze.crm_sales_info
        

        TRUNCATE TABLE Silver.erp_cust_az12 
        INSERT INTO Silver.erp_cust_az12(cid,bdate,gen)
        SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
            ELSE cid
        END cid,
        CASE WHEN bdate > GETDATE() THEN Null
            ELSE bdate
        END AS bdate,
        CASE WHEN SUBSTRING(gen,1,1) = 'M' THEN 'MALE'
            WHEN SUBSTRING(gen,1,1) = 'F' THEN 'FEMALE'
        ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12
        SELECT *
        FROM Silver.erp_cust_az12


        TRUNCATE TABLE silver.erp_loc_a101
        INSERT INTO silver.erp_loc_a101(cid,cntry)

        SELECT 
        REPLACE(cid,'-','') AS cid,
        CASE WHEN SUBSTRING(cntry,1,1) = 'A' THEN 'Australia'
            WHEN SUBSTRING(cntry,1,2) = 'US' THEN 'United States'
            WHEN SUBSTRING(cntry,1,3) = 'USA' THEN 'United States'
            WHEN SUBSTRING(cntry,1,13) = 'United States' THEN 'United States'
            WHEN SUBSTRING(cntry,1,14) = 'United Kingdom' THEN 'United Kingdom'
            WHEN SUBSTRING(cntry,1,1) = 'C' THEN 'Canada'
            WHEN SUBSTRING(cntry,1,2) = 'DE' THEN 'Germany'
            WHEN SUBSTRING(cntry,1,1) = 'G' THEN 'Germany'
            WHEN SUBSTRING(cntry,1,1) = 'F' THEN 'France'
            ELSE 'n/a'
        END AS country
        FROM bronze.erp_loc_a101



        TRUNCATE TABLE silver.erp_px_cat_g1v2 
        Insert INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintainance)
        SELECT 
        id,
        cat,
        subcat,
        CASE WHEN SUBSTRING(maintainance,1,1) = 'Y' THEN 'YES'
            WHEN SUBSTRING(maintainance,1,1) = 'N' THEN 'NO'
        END AS maintainance
        FROM bronze.erp_px_cat_g1v2
END
