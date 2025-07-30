/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @Load_Start_time DATETIME, @Load_End_time DATETIME,
	@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time=GETDATE();
			PRINT '====================================================================';
			PRINT 'LOADING THE BRONZE LAYER';
			PRINT '====================================================================';

			PRINT '------------------------------------------------------------------';
			PRINT 'LOADING CRM TABLES';
			PRINT '-------------------------------------------------------------------';

			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info 
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();

			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';


			--bronze.crm_prd_info

			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info 
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();
			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';
			

			--Sales details
			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details 
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();
			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';

			--ERP source tables bulk insertion--

			--bronze.erp_cust_az12 table--

			PRINT '------------------------------------------------------------------';
			PRINT 'LOADING ERP TABLES';
			PRINT '-------------------------------------------------------------------';

			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.erp_cust_az12;
			BULK INSERT bronze.erp_cust_az12
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();
			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';

			--bronze.erp_loc_a101 table--
			
			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.erp_loc_a101;
			BULK INSERT bronze.erp_loc_a101
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();
			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';

			--bronze.erp_px_cat_g1v2  table--
			
			SET @Load_Start_time= GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			BULK INSERT bronze.erp_px_cat_g1v2 
			FROM
			'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH
			(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
			SET @Load_End_time= GETDATE();
			PRINT '>> LOAD DURATION IS :' +CAST(DATEDIFF(SECOND, @Load_Start_time,@Load_End_time) AS NVARCHAR) + ' seconds';
			SET @batch_end_time=GETDATE();

			PRINT'==================================================';
			PRINT 'BRONZE LAYER LOAD IS COMPLETED';
			PRINT 'Total duration for loading bronze layer is :' +CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=================================================';
	END TRY
	BEGIN CATCH
			PRINT '==============================================';
			PRINT 'An error occured during loading bronze layer';
			PRINT 'Error message :' +ERROR_MESSAGE();
			PRINT 'Error Number :' +CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State :' +CAST(ERROR_STATE() AS NVARCHAR); 
			PRINT '================================================';
	END CATCH

END
