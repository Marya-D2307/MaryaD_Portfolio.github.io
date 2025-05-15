-- ============================================
-- DATAFLOW2025 - FORECASTING BUSINESS PERFORMANCE
-- ============================================

-- ============================================
-- DATA CLEANING
-- ============================================


USE Project_03_BA;

-- ============================================
-- 1. Preview Source Tables
-- ============================================
SELECT TOP 100 * FROM dbo.geography;
SELECT TOP 100 * FROM dbo.product;
SELECT TOP 100 * FROM dbo.train;
SELECT TOP 100 * FROM dbo.test;

-- ============================================
-- 2. Inspect Column Data Types
-- ============================================
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'train';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'test';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'product';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'geography';

-- ============================================
-- 3. Merge and Create Unified Dataset
-- ============================================
DROP TABLE IF EXISTS merged_data;

CREATE TABLE merged_data (
    Date DATE,
    ProductID VARCHAR(50),
    Product VARCHAR(255),
    Category VARCHAR(100),
    Segment VARCHAR(100),
    Zip VARCHAR(10),
    City VARCHAR(100),
    State VARCHAR(100),
    Region VARCHAR(50),
    Units INT,
    Revenue DECIMAL(10,2),
    COGS DECIMAL(10,2)
);

INSERT INTO merged_data
SELECT 
    s.Date, 
    s.ProductID, 
    p.Product, 
    p.Category, 
    p.Segment, 
    s.Zip, 
    g.City, 
    g.State, 
    g.Region, 
    TRY_CAST(s.Units AS INT), 
    TRY_CAST(s.Revenue AS DECIMAL(10,2)), 
    TRY_CAST(s.COGS AS DECIMAL(10,2))
FROM (
    SELECT * FROM dbo.train
    UNION ALL
    SELECT * FROM dbo.test
) s
JOIN dbo.product p ON s.ProductID = p.ProductID
JOIN dbo.geography g ON s.Zip = g.Zip;

-- ============================================
-- 4. Data Cleaning & Validation
-- ============================================

-- Check for Missing Values
SELECT 
    COUNT(*) AS Total_Rows,
    SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) AS Null_Dates,
    SUM(CASE WHEN ProductID IS NULL THEN 1 ELSE 0 END) AS Null_ProductID,
    SUM(CASE WHEN Product IS NULL THEN 1 ELSE 0 END) AS Null_Product,
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS Null_Category,
    SUM(CASE WHEN Segment IS NULL THEN 1 ELSE 0 END) AS Null_Segment,
    SUM(CASE WHEN Zip IS NULL THEN 1 ELSE 0 END) AS Null_Zip,
    SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS Null_City,
    SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS Null_State,
    SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END) AS Null_Region,
    SUM(CASE WHEN Units IS NULL THEN 1 ELSE 0 END) AS Null_Units,
    SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS Null_Revenue,
    SUM(CASE WHEN COGS IS NULL THEN 1 ELSE 0 END) AS Null_COGS
FROM merged_data;

-- Set NULL Revenue to 0 if applicable
UPDATE merged_data
SET Revenue = 0
WHERE Revenue IS NULL;

-- Remove Duplicate Records Based on ProductID, Zip, and Date
WITH CTE_Duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ProductID, Zip, Date ORDER BY (SELECT NULL)) AS RowNum
    FROM merged_data
)
DELETE FROM merged_data
WHERE EXISTS (
    SELECT 1
    FROM CTE_Duplicates d
    WHERE d.RowNum > 1
      AND d.ProductID = merged_data.ProductID
      AND d.Zip = merged_data.Zip
      AND d.Date = merged_data.Date
);

-- Detect Outliers in Revenue
SELECT * 
FROM merged_data 
WHERE Revenue < 0 
   OR Revenue > (SELECT AVG(Revenue) + 3 * STDEV(Revenue) FROM merged_data);

-- ============================================
-- 5. Descriptive Analysis
-- ============================================

-- Total Revenue and Units Sold
SELECT
    SUM(Revenue) AS Total_Revenue,
    SUM(Units) AS Total_Units_Sold
FROM merged_data;

-- Monthly and Yearly Revenue Trends
SELECT 
    YEAR(Date) AS Year, 
    MONTH(Date) AS Month, 
    SUM(Revenue) AS Total_Revenue,  
    SUM(Units) AS Total_Units_Sold
FROM merged_data
GROUP BY YEAR(Date), MONTH(Date)
ORDER BY Year, Month;

-- Revenue and Units by Product
SELECT 
    ProductID, 
    Product, 
    SUM(Revenue) AS Total_Revenue,
    SUM(Units) AS Total_Units_Sold
FROM merged_data
GROUP BY ProductID, Product
ORDER BY Total_Revenue DESC;

-- Total Profit and Gross Margin
SELECT 
    SUM(Revenue - COGS) AS Total_Profit,
    (SUM(Revenue - COGS) * 100.0 / NULLIF(SUM(Revenue), 0)) AS Gross_Margin_Percentage
FROM merged_data;

-- Product Demand (Units Sold)
SELECT 
    ProductID, 
    Product, 
    SUM(Units) AS Total_Units_Sold
FROM merged_data
GROUP BY ProductID, Product
ORDER BY Total_Units_Sold DESC;

-- Final Preview of Merged Dataset
SELECT TOP 1000 * FROM merged_data;
