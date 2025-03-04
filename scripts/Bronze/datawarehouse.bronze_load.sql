CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    -- Declare variables to store start and end times, batch start and end times, message, and status
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @message NVARCHAR(100), @status NVARCHAR(10);

    BEGIN TRY
        -- Print statements to indicate the start of the bronze layer loading process
        PRINT('=======================================================================');
        PRINT('Loading the bronze layer');
        PRINT('=======================================================================');

        PRINT('-----------------------------------------------------------------------');
        PRINT('Loading CRM tables');
        PRINT('-----------------------------------------------------------------------');

        -- Set batch start time
        SET @batch_start_time = GETDATE();

        -- Load CRM customer information table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.crm_cust_info');
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT('>> Inserting data into : bronze.crm_cust_info');
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.crm_cust_info', DATEDIFF(SECOND, @start_time, @end_time));

        -- Load CRM product information table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.crm_prd_info');
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT('>> Inserting data into : bronze.crm_prd_info');
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.crm_prd_info', DATEDIFF(SECOND, @start_time, @end_time));

        -- Load CRM sales details table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.crm_sales_details');
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT('>> Inserting data into : bronze.crm_sales_details');
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.crm_sales_details', DATEDIFF(SECOND, @start_time, @end_time));

        PRINT('-----------------------------------------------------------------------');
        PRINT('Loading ERP tables');
        PRINT('-----------------------------------------------------------------------');

        -- Load ERP customer information table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.erp_cust_az12');
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT('>> Inserting data into : bronze.erp_cust_az12');
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.erp_cust_az12', DATEDIFF(SECOND, @start_time, @end_time));

        -- Load ERP location information table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.erp_loc_a101');
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT('>> Inserting data into : bronze.erp_loc_a101');
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.erp_loc_a101', DATEDIFF(SECOND, @start_time, @end_time));

        -- Load ERP product category information table
        SET @start_time = GETDATE();
        PRINT('>> Truncating table : bronze.erp_px_cat_g1v2');
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT('>> Inserting data into : bronze.erp_px_cat_g1v2');
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\adraj\Downloads\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('>> Load time :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        -- Uncomment the following line to log the load time
        -- INSERT INTO log_table (table_name, load_time) VALUES ('bronze.erp_px_cat_g1v2', DATEDIFF(SECOND, @start_time, @end_time));

        -- Set batch end time and print completion message
        SET @batch_end_time = GETDATE();
        PRINT('Loading Bronze layer is complete');
        PRINT('>> Batch processing time :' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');

        -- Insert batch processing time and success message into batch_log_table
        SET @message = 'Success';
        SET @status = 'Success';
        INSERT INTO batch_log_table (batch_start_time, batch_end_time, batch_load_time, message, status)
        VALUES (@batch_start_time, @batch_end_time, DATEDIFF(SECOND, @batch_start_time, @batch_end_time), @message, @status);

    END TRY
    BEGIN CATCH
        -- Print error messages in case of failure
        PRINT('=====================================================================');
        PRINT('Error occurred during loading bronze layer');
        PRINT('Error Message :' + ERROR_MESSAGE());
        PRINT('Error Number :' + CAST(ERROR_NUMBER() AS NVARCHAR));
        PRINT('Error State :' + CAST(ERROR_STATE() AS NVARCHAR));
        PRINT('=====================================================================');

        -- Insert error message and failure status into batch_log_table
        SET @message = ERROR_MESSAGE();
        SET @status = 'Failure';
        INSERT INTO batch_log_table (batch_start_time, batch_end_time, batch_load_time, message, status)
        VALUES (@batch_start_time, GETDATE(), DATEDIFF(SECOND, @batch_start_time, GETDATE()), @message, @status);
    END CATCH
END
