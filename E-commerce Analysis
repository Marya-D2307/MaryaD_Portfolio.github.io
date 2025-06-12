USE ecomm_02_train;
GO

-------------------------------
-- 1. CREATE UNIFIED VIEW
-------------------------------
CREATE VIEW dbo.ecomm_master AS
SELECT
    -- Order Info
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_timestamp,
    o.order_estimated_delivery_date,
    o.order_status,

    -- Customer Info
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix,

    -- Order Items
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.shipping_charges,

    -- Product Info
    p.product_category_name,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_weight_g,

    -- Payment Info
    pay.payment_type,
    pay.payment_installments,
    pay.payment_value

FROM dbo.df_Orders o
LEFT JOIN dbo.df_Customers c ON o.customer_id = c.customer_id
LEFT JOIN dbo.df_OrderItems oi ON o.order_id = oi.order_id
LEFT JOIN dbo.df_Products p ON oi.product_id = p.product_id
LEFT JOIN dbo.df_Payments pay ON o.order_id = pay.order_id;
GO

-------------------------------
-- 2. DATA CLEANING VIEW
-------------------------------
CREATE VIEW dbo.ecomm_master_clean AS
SELECT
    order_id,
    customer_id,
    TRY_CAST(order_purchase_timestamp AS DATETIME) AS order_purchase_timestamp,
    TRY_CAST(order_approved_at AS DATETIME) AS order_approved_at,
    TRY_CAST(order_delivered_timestamp AS DATETIME) AS order_delivered_timestamp,
    TRY_CAST(order_estimated_delivery_date AS DATETIME) AS order_estimated_delivery_date,
    order_status,
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    product_id,
    seller_id,
    TRY_CAST(price AS FLOAT) AS price,
    TRY_CAST(shipping_charges AS FLOAT) AS shipping_charges,
    product_category_name,
    TRY_CAST(product_length_cm AS FLOAT) AS product_length_cm,
    TRY_CAST(product_height_cm AS FLOAT) AS product_height_cm,
    TRY_CAST(product_width_cm AS FLOAT) AS product_width_cm,
    TRY_CAST(product_weight_g AS FLOAT) AS product_weight_g,
    payment_type,
    TRY_CAST(payment_installments AS INT) AS payment_installments,
    TRY_CAST(payment_value AS FLOAT) AS payment_value
FROM dbo.ecomm_master
WHERE order_id IS NOT NULL
  AND TRY_CAST(order_purchase_timestamp AS DATETIME) IS NOT NULL;
GO

-------------------------------
-- 3. CHECKING FOR MISSING DATA
-------------------------------
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_delivered_timestamp IS NULL THEN 1 ELSE 0 END) AS missing_delivery_time,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS missing_estimated_date,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS missing_order_status,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) AS missing_category,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS missing_payment_type
FROM dbo.ecomm_master_clean;

-------------------------------
-- 4. DUPLICATE CHECKS
-------------------------------
-- Orders with duplicate product rows
SELECT order_id, product_id, COUNT(*) AS row_count
FROM dbo.ecomm_master_clean
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;

-- Are they bulk or repeated purchases?
SELECT 
    customer_id,
    product_id,
    COUNT(*) AS times_ordered,
    SUM(price) AS total_price,
    SUM(shipping_charges) AS total_shipping,
    SUM(payment_value) AS total_payment
FROM dbo.ecomm_master_clean
GROUP BY customer_id, product_id
HAVING COUNT(*) > 1
ORDER BY times_ordered DESC;

-------------------------------
-- 5. CUSTOMER BEHAVIOR ANALYSIS
-------------------------------

-- Customers who repeat purchases of the same product
SELECT 
    customer_id,
    product_id,
    COUNT(DISTINCT order_id) AS times_purchased
FROM dbo.ecomm_master_clean
GROUP BY customer_id, product_id
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY times_purchased DESC;

-- Identify potential business/wholesale buyers
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items_purchased,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(price) AS total_spent
FROM dbo.ecomm_master_clean
GROUP BY customer_id
HAVING 
    COUNT(DISTINCT order_id) > 10 
    OR COUNT(*) > 20 
    OR SUM(price) > 5000
ORDER BY total_spent DESC;

-- Repeat purchase behavior by category and state
SELECT 
    product_category_name,
    customer_state,
    COUNT(DISTINCT order_id) AS total_repeat_orders
FROM (
    SELECT customer_id, product_id, order_id, product_category_name, customer_state
    FROM dbo.ecomm_master_clean
    GROUP BY customer_id, product_id, order_id, product_category_name, customer_state
) AS base
GROUP BY product_category_name, customer_state
HAVING COUNT(*) > 1
ORDER BY total_repeat_orders DESC;

-------------------------------
-- 6. CREATE SEGMENT: High-Frequency Buyers
-------------------------------
DROP TABLE IF EXISTS high_freq_buyers;
GO

CREATE TABLE high_freq_buyers (
    customer_id VARCHAR(255),
    total_orders INT,
    distinct_products_bought INT,
    total_items_bought INT,
    total_spent FLOAT
);
GO

INSERT INTO high_freq_buyers
SELECT
    customer_id,
    COUNT(DISTINCT order_id),
    COUNT(DISTINCT product_id),
    COUNT(*),
    SUM(price)
FROM dbo.ecomm_master_clean
GROUP BY customer_id
HAVING 
    COUNT(DISTINCT order_id) >= 15
    OR COUNT(*) >= 30
    OR SUM(price) >= 3000;

-- View data for dashboard/export
SELECT *
FROM high_freq_buyers;

-- Join for full data
SELECT emc.*
FROM dbo.ecomm_master_clean emc
JOIN high_freq_buyers hfb ON emc.customer_id = hfb.customer_id;

-------------------------------
-- 7. DEMAND FORECAST INSIGHTS
-------------------------------
SELECT 
    product_id,
    COUNT(DISTINCT hfb.customer_id) AS repeat_buyers,
    COUNT(emc.order_id) AS total_orders,
    SUM(emc.price) AS total_revenue
FROM dbo.ecomm_master_clean emc
JOIN high_freq_buyers hfb ON emc.customer_id = hfb.customer_id
GROUP BY product_id
HAVING 
    COUNT(DISTINCT hfb.customer_id) >= 10
    AND COUNT(emc.order_id) >= 20
ORDER BY repeat_buyers DESC, total_orders DESC;
