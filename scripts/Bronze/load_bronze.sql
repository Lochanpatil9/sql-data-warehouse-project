
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

PRINT '=========================================';
PRINT ' Loading Bronze Layer ';
PRINT '=========================================';
PRINT ' ---------------------------------------- ';
PRINT 'Loading CRM Tables';
PRINT ' ---------------------------------------- ';
PRINT '>> Truncating table : bronze.crm_cust_info ';
TRUNCATE TABLE bronze.crm_cust_info;
PRINT '>> Inserting data into table : bronze.crm_cust_info ';
BULK INSERT bronze.crm_cust_info
From '/var/opt/mssql/data/csv/cust_info.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
PRINT '>> Truncating table : bronze.crm_prd_info ';
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '>> Inserting data into table : bronze.crm_prd_info ';
BULK INSERT bronze.crm_prd_info
From '/var/opt/mssql/data/csv/prd_info.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
PRINT '>> Truncating table : bronze.crm_sales_info ';
TRUNCATE TABLE bronze.crm_sales_info;
PRINT '>> Inserting data into table : bronze.crm_sales_info ';
BULK INSERT bronze.crm_sales_info
From '/var/opt/mssql/data/csv/sales_details.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT ' ---------------------------------------- ';
PRINT 'Loading ERP Tables';
PRINT ' ---------------------------------------- ';
 PRINT '>> Truncating table : bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT '>> Inserting data into table : bronze.erp_loc_a101 ';
BULK INSERT bronze.erp_loc_a101
From '/var/opt/mssql/data/csv/LOC_A101.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
PRINT '>> Truncating table : bronze.erp_cust_az12 ';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '>> Inserting data into table : bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
From '/var/opt/mssql/data/csv/CUST_AZ12.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
PRINT '>> Truncating table : bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '>> Inserting data into table : bronze.erp_px_cat_g1v2 ';
BULK INSERT bronze.erp_px_cat_g1v2
From '/var/opt/mssql/data/csv/PX_CAT_G1V2.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
END TRY
BEGIN CATCH 
PRINT ' ---------------------------------------- ';
PRINT ' Error Occured During loading bronze Layer ' ;
PRINT 'Error Messsage' + ERROR_MESSAGE();
PRINT 'Error Messsage' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Messsage' + CAST(ERROR_STAKE() AS NVARCHAR);
PRINT ' ---------------------------------------- ';

END CATCH
END
