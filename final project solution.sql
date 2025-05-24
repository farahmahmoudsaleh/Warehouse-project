-- ================================================================
-- DATA WAREHOUSE HOMEWORK #2 - COMPLETE SOLUTION
-- Student: Farah Saleh
-- Course: Data Warehouse - Second Semester 2024/2025
-- ================================================================

-- Schema Design Justification:
-- For this project, a **Star Schema** was chosen due to its simplicity and performance advantages.
-- Each fact table is directly linked to its dimension tables using surrogate keys.
-- This design ensures fast query performance for analytical workloads, which is ideal for reporting and aggregations.
-- 
-- Fact Table 1: fact_monthly_payment (measures: total payment amount)
-- - Linked dimensions: dim_date, dim_staff, dim_rental
-- 
-- Fact Table 2: fact_daily_inventory (measures: inventory count or availability)
-- - Linked dimensions: dim_date, dim_film, dim_store
--
-- A Snowflake Schema was not used to avoid excessive joins and complexity in query execution time.

-- ================================================================
-- PART 1: DIMENSIONAL MODEL DESIGN
-- ================================================================

-- Create Data Warehouse Database
CREATE DATABASE IF NOT EXISTS dw_rental;
USE dw_rental;

-- ================================================================
-- DIMENSION TABLES CREATION
-- ================================================================

-- 1. Date Dimension Table
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BOOLEAN
);

-- 2. Staff Dimension Table
CREATE TABLE dim_staff (
    staff_key INT AUTO_INCREMENT PRIMARY KEY,
    staff_id INT UNIQUE,
    first_name VARCHAR(45),
    last_name VARCHAR(45),
    email VARCHAR(50),
    store_id INT,
    active BOOLEAN,
    username VARCHAR(16),
    address_line1 VARCHAR(50),
    address_line2 VARCHAR(50),
    district VARCHAR(20),
    city VARCHAR(50),
    postal_code VARCHAR(10),
    phone VARCHAR(20),
    country VARCHAR(50)
);

-- 3. Rental Dimension Table
CREATE TABLE dim_rental (
    rental_key INT AUTO_INCREMENT PRIMARY KEY,
    rental_id INT UNIQUE,
    rental_date DATETIME,
    return_date DATETIME,
    customer_id INT,
    customer_first_name VARCHAR(45),
    customer_last_name VARCHAR(45),
    customer_email VARCHAR(50),
    film_id INT,
    film_title VARCHAR(255),
    film_category VARCHAR(25),
    rental_duration INT
);

-- 4. Film Dimension Table
CREATE TABLE dim_film (
    film_key INT AUTO_INCREMENT PRIMARY KEY,
    film_id INT UNIQUE,
    title VARCHAR(255),
    description TEXT,
    release_year YEAR,
    language VARCHAR(20),
    original_language VARCHAR(20),
    rental_duration INT,
    rental_rate DECIMAL(4,2),
    length INT,
    replacement_cost DECIMAL(5,2),
    rating VARCHAR(10),
    special_features SET('Trailers','Commentaries','Deleted Scenes','Behind the Scenes'),
    category_name VARCHAR(25)
);

-- 5. Store Dimension Table
CREATE TABLE dim_store (
    store_key INT AUTO_INCREMENT PRIMARY KEY,
    store_id INT UNIQUE,
    manager_staff_id INT,
    manager_first_name VARCHAR(45),
    manager_last_name VARCHAR(45),
    address_line1 VARCHAR(50),
    address_line2 VARCHAR(50),
    district VARCHAR(20),
    city VARCHAR(50),
    postal_code VARCHAR(10),
    phone VARCHAR(20),
    country VARCHAR(50)
);

-- ================================================================
-- FACT TABLES CREATION
-- ================================================================

-- Fact Table 1: Monthly Payment per Staff per Rent
CREATE TABLE fact_monthly_payment (
    payment_date_key INT,
    staff_key INT,
    rental_key INT,
    total_payment_amount DECIMAL(8,2),
    rental_count INT,
    avg_payment_per_rental DECIMAL(6,2),
    
    PRIMARY KEY (payment_date_key, staff_key, rental_key),
    FOREIGN KEY (payment_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key),
    FOREIGN KEY (rental_key) REFERENCES dim_rental(rental_key)
);

-- Fact Table 2: Daily Inventory per Film per Store
CREATE TABLE fact_daily_inventory (
    inventory_date_key INT,
    film_key INT,
    store_key INT,
    available_inventory_count INT,
    total_inventory_count INT,
    utilization_rate DECIMAL(5,2),
    
    PRIMARY KEY (inventory_date_key, film_key, store_key),
    FOREIGN KEY (inventory_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (film_key) REFERENCES dim_film(film_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
);

-- ================================================================
-- PART 2: ETL DEVELOPMENT
-- ================================================================

-- ================================================================
-- STEP 1: POPULATE DATE DIMENSION
-- ================================================================

DELIMITER //
CREATE PROCEDURE PopulateDateDimension(start_date DATE, end_date DATE)
BEGIN
    DECLARE current_date DATE DEFAULT start_date;
    
    WHILE current_date <= end_date DO
        INSERT IGNORE INTO dim_date (
            date_key, full_date, year, quarter, month, month_name,
            day, day_of_week, day_name, week_of_year, is_weekend
        ) VALUES (
            DATE_FORMAT(current_date, '%Y%m%d'),
            current_date,
            YEAR(current_date),
            QUARTER(current_date),
            MONTH(current_date),
            MONTHNAME(current_date),
            DAY(current_date),
            DAYOFWEEK(current_date),
            DAYNAME(current_date),
            WEEK(current_date),
            CASE WHEN DAYOFWEEK(current_date) IN (1,7) THEN TRUE ELSE FALSE END
        );
        
        SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
    END WHILE;
END//
DELIMITER ;

-- Execute the procedure to populate date dimension
CALL PopulateDateDimension('2005-01-01', '2025-12-31');

-- ================================================================
-- STEP 2: POPULATE STAFF DIMENSION
-- ================================================================

-- Handle redundant data and missing values for Staff
INSERT INTO dim_staff (
    staff_id, first_name, last_name, email, store_id, active, username,
    address_line1, district, city, postal_code, phone, country
)
SELECT DISTINCT
    s.staff_id,
    COALESCE(s.first_name, 'Unknown') as first_name,
    COALESCE(s.last_name, 'Unknown') as last_name,
    COALESCE(NULLIF(s.email, ''), CONCAT('staff_', s.staff_id, '@company.com')) as email,
    s.store_id,
    COALESCE(s.active, 1) as active,
    COALESCE(NULLIF(s.username, ''), CONCAT('user_', s.staff_id)) as username,
    COALESCE(a.address, 'Address not available') as address_line1,
    COALESCE(a.district, 'Unknown') as district,
    COALESCE(c.city, 'Unknown') as city,
    COALESCE(NULLIF(a.postal_code, ''), '00000') as postal_code,
    COALESCE(NULLIF(a.phone, ''), 'Not provided') as phone,
    COALESCE(co.country, 'Unknown') as country
FROM sakila.staff s
LEFT JOIN sakila.address a ON s.address_id = a.address_id
LEFT JOIN sakila.city c ON a.city_id = c.city_id
LEFT JOIN sakila.country co ON c.country_id = co.country_id
ON DUPLICATE KEY UPDATE
    first_name = VALUES(first_name),
    last_name = VALUES(last_name),
    email = VALUES(email),
    active = VALUES(active);

-- ================================================================
-- STEP 3: POPULATE FILM DIMENSION
-- ================================================================

-- Handle redundant data and missing values for Film
INSERT INTO dim_film (
    film_id, title, description, release_year, language, rental_duration,
    rental_rate, length, replacement_cost, rating, special_features, category_name
)
SELECT DISTINCT
    f.film_id,
    COALESCE(f.title, 'Unknown Title') as title,
    COALESCE(NULLIF(f.description, ''), 'No description available') as description,
    COALESCE(f.release_year, 2000) as release_year,
    COALESCE(l.name, 'English') as language,
    COALESCE(f.rental_duration, 3) as rental_duration,
    COALESCE(f.rental_rate, 0.99) as rental_rate,
    COALESCE(f.length, 90) as length,
    COALESCE(f.replacement_cost, 19.99) as replacement_cost,
    COALESCE(f.rating, 'G') as rating,
    f.special_features,
    COALESCE(cat.name, 'Uncategorized') as category_name
FROM sakila.film f
LEFT JOIN sakila.language l ON f.language_id = l.language_id
LEFT JOIN sakila.film_category fc ON f.film_id = fc.film_id
LEFT JOIN sakila.category cat ON fc.category_id = cat.category_id
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    description = VALUES(description),
    category_name = VALUES(category_name);

-- ================================================================
-- STEP 4: POPULATE STORE DIMENSION
-- ================================================================

-- Handle redundant data and missing values for Store
INSERT INTO dim_store (
    store_id, manager_staff_id, manager_first_name, manager_last_name,
    address_line1, district, city, postal_code, phone, country
)
SELECT DISTINCT
    st.store_id,
    st.manager_staff_id,
    COALESCE(s.first_name, 'Unknown') as manager_first_name,
    COALESCE(s.last_name, 'Manager') as manager_last_name,
    COALESCE(a.address, 'Address not available') as address_line1,
    COALESCE(a.district, 'Unknown') as district,
    COALESCE(c.city, 'Unknown') as city,
    COALESCE(NULLIF(a.postal_code, ''), '00000') as postal_code,
    COALESCE(NULLIF(a.phone, ''), 'Not provided') as phone,
    COALESCE(co.country, 'Unknown') as country
FROM sakila.store st
LEFT JOIN sakila.staff s ON st.manager_staff_id = s.staff_id
LEFT JOIN sakila.address a ON st.address_id = a.address_id
LEFT JOIN sakila.city c ON a.city_id = c.city_id
LEFT JOIN sakila.country co ON c.country_id = co.country_id
ON DUPLICATE KEY UPDATE
    manager_first_name = VALUES(manager_first_name),
    manager_last_name = VALUES(manager_last_name);

-- ================================================================
-- STEP 5: POPULATE RENTAL DIMENSION
-- ================================================================

-- Handle redundant data and missing values for Rental
INSERT INTO dim_rental (
    rental_id, rental_date, return_date, customer_id, 
    customer_first_name, customer_last_name, customer_email,
    film_id, film_title, film_category, rental_duration
)
SELECT DISTINCT
    r.rental_id,
    r.rental_date,
    r.return_date,
    r.customer_id,
    COALESCE(cust.first_name, 'Unknown') as customer_first_name,
    COALESCE(cust.last_name, 'Customer') as customer_last_name,
    COALESCE(NULLIF(cust.email, ''), CONCAT('customer_', r.customer_id, '@unknown.com')) as customer_email,
    f.film_id,
    COALESCE(f.title, 'Unknown Film') as film_title,
    COALESCE(cat.name, 'Uncategorized') as film_category,
    COALESCE(f.rental_duration, 3) as rental_duration
FROM sakila.rental r
LEFT JOIN sakila.customer cust ON r.customer_id = cust.customer_id
LEFT JOIN sakila.inventory inv ON r.inventory_id = inv.inventory_id
LEFT JOIN sakila.film f ON inv.film_id = f.film_id
LEFT JOIN sakila.film_category fc ON f.film_id = fc.film_id
LEFT JOIN sakila.category cat ON fc.category_id = cat.category_id
WHERE r.rental_date IS NOT NULL
ON DUPLICATE KEY UPDATE
    customer_first_name = VALUES(customer_first_name),
    customer_last_name = VALUES(customer_last_name),
    customer_email = VALUES(customer_email);

-- ================================================================
-- STEP 6: POPULATE FACT TABLE 1 - MONTHLY PAYMENT
-- ================================================================

-- ETL for Monthly Payment per Staff per Rent
INSERT INTO fact_monthly_payment (
    payment_date_key, staff_key, rental_key, 
    total_payment_amount, rental_count, avg_payment_per_rental
)
SELECT 
    DATE_FORMAT(p.payment_date, '%Y%m%d') as payment_date_key,
    ds.staff_key,
    dr.rental_key,
    SUM(COALESCE(p.amount, 0)) as total_payment_amount,
    COUNT(DISTINCT r.rental_id) as rental_count,
    AVG(COALESCE(p.amount, 0)) as avg_payment_per_rental
FROM sakila.payment p
JOIN sakila.rental r ON p.rental_id = r.rental_id
JOIN dim_staff ds ON r.staff_id = ds.staff_id
JOIN dim_rental dr ON r.rental_id = dr.rental_id
JOIN dim_date dd ON DATE_FORMAT(p.payment_date, '%Y%m%d') = dd.date_key
WHERE p.amount IS NOT NULL 
  AND p.payment_date IS NOT NULL
  AND r.rental_date IS NOT NULL
GROUP BY 
    DATE_FORMAT(p.payment_date, '%Y%m%d'),
    ds.staff_key,
    dr.rental_key
HAVING SUM(COALESCE(p.amount, 0)) > 0
ON DUPLICATE KEY UPDATE
    total_payment_amount = VALUES(total_payment_amount),
    rental_count = VALUES(rental_count),
    avg_payment_per_rental = VALUES(avg_payment_per_rental);

-- ================================================================
-- STEP 7: POPULATE FACT TABLE 2 - DAILY INVENTORY
-- ================================================================

-- ETL for Daily Inventory per Film per Store
INSERT INTO fact_daily_inventory (
    inventory_date_key, film_key, store_key,
    available_inventory_count, total_inventory_count, utilization_rate
)
SELECT 
    dd.date_key as inventory_date_key,
    df.film_key,
    dst.store_key,
    COUNT(CASE WHEN inv.inventory_id NOT IN (
        SELECT DISTINCT inventory_id 
        FROM sakila.rental 
        WHERE rental_date <= dd.full_date 
        AND (return_date IS NULL OR return_date > dd.full_date)
    ) THEN 1 END) as available_inventory_count,
    COUNT(inv.inventory_id) as total_inventory_count,
    CASE 
        WHEN COUNT(inv.inventory_id) > 0 THEN
            ROUND((COUNT(inv.inventory_id) - COUNT(CASE WHEN inv.inventory_id NOT IN (
                SELECT DISTINCT inventory_id 
                FROM sakila.rental 
                WHERE rental_date <= dd.full_date 
                AND (return_date IS NULL OR return_date > dd.full_date)
            ) THEN 1 END)) * 100.0 / COUNT(inv.inventory_id), 2)
        ELSE 0 
    END as utilization_rate
FROM dim_date dd
CROSS JOIN sakila.inventory inv
JOIN dim_film df ON inv.film_id = df.film_id
JOIN dim_store dst ON inv.store_id = dst.store_id
WHERE dd.full_date BETWEEN '2005-05-24' AND '2006-03-01'
GROUP BY dd.date_key, df.film_key, dst.store_key
HAVING COUNT(inv.inventory_id) > 0
ON DUPLICATE KEY UPDATE
    available_inventory_count = VALUES(available_inventory_count),
    total_inventory_count = VALUES(total_inventory_count),
    utilization_rate = VALUES(utilization_rate);

-- ================================================================
-- STEP 8: CREATE INDEXES FOR PERFORMANCE
-- ================================================================

-- Indexes for Fact Table 1
CREATE INDEX idx_fact_payment_date ON fact_monthly_payment(payment_date_key);
CREATE INDEX idx_fact_payment_staff ON fact_monthly_payment(staff_key);
CREATE INDEX idx_fact_payment_rental ON fact_monthly_payment(rental_key);
CREATE INDEX idx_payment_date_staff ON fact_monthly_payment(payment_date_key, staff_key);

-- Indexes for Fact Table 2
CREATE INDEX idx_fact_inventory_date ON fact_daily_inventory(inventory_date_key);
CREATE INDEX idx_fact_inventory_film ON fact_daily_inventory(film_key);
CREATE INDEX idx_fact_inventory_store ON fact_daily_inventory(store_key);
CREATE INDEX idx_inventory_date_film ON fact_daily_inventory(inventory_date_key, film_key);

-- Indexes for Dimension Tables
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);
CREATE INDEX idx_dim_staff_store ON dim_staff(store_id);
CREATE INDEX idx_dim_film_category ON dim_film(category_name);
CREATE INDEX idx_dim_rental_customer ON dim_rental(customer_id);
CREATE INDEX idx_dim_rental_film ON dim_rental(film_id);

-- ================================================================
-- STEP 9: DATA QUALITY VALIDATION
-- ================================================================

-- Check for orphaned records in fact tables
SELECT 'Orphaned Payment Records' as check_type, COUNT(*) as count
FROM fact_monthly_payment f
LEFT JOIN dim_date d ON f.payment_date_key = d.date_key
WHERE d.date_key IS NULL

UNION ALL

SELECT 'Orphaned Staff Records' as check_type, COUNT(*) as count
FROM fact_monthly_payment f
LEFT JOIN dim_staff s ON f.staff_key = s.staff_key
WHERE s.staff_key IS NULL

UNION ALL

SELECT 'Orphaned Inventory Records' as check_type, COUNT(*) as count
FROM fact_daily_inventory f
LEFT JOIN dim_film df ON f.film_key = df.film_key
WHERE df.film_key IS NULL;

-- Validate data completeness
SELECT 
    'Staff Dimension' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN first_name = 'Unknown' THEN 1 END) as unknown_first_names,
    COUNT(CASE WHEN email LIKE '%@company.com' THEN 1 END) as generated_emails
FROM dim_staff

UNION ALL

SELECT 
    'Film Dimension' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN description = 'No description available' THEN 1 END) as missing_descriptions,
    COUNT(CASE WHEN category_name = 'Uncategorized' THEN 1 END) as uncategorized_films
FROM dim_film

UNION ALL

SELECT 
    'Rental Dimension' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN customer_email LIKE '%@unknown.com' THEN 1 END) as generated_customer_emails,
    COUNT(CASE WHEN return_date IS NULL THEN 1 END) as unreturned_rentals
FROM dim_rental;

-- ================================================================
-- STEP 10: TEST WITH DIRTY DATA
-- ================================================================

-- Insert test data with quality issues to test ETL robustness
INSERT INTO sakila.staff (staff_id, first_name, last_name, email, store_id, active, address_id)
VALUES 
(99, NULL, 'TestStaff', '', 1, 1, 1),  -- Missing first name, empty email
(100, 'Duplicate', 'Staff', 'test@test.com', 1, 1, 1),
(101, '', 'EmptyFirstName', NULL, 2, 1, 2);  -- Empty first name, NULL email

-- Re-run Staff ETL to test handling of dirty data
INSERT INTO dim_staff (
    staff_id, first_name, last_name, email, store_id, active, username,
    address_line1, district, city, postal_code, phone, country
)
SELECT DISTINCT
    s.staff_id,
    COALESCE(NULLIF(s.first_name, ''), 'Unknown') as first_name,
    COALESCE(NULLIF(s.last_name, ''), 'Unknown') as last_name,
    COALESCE(NULLIF(s.email, ''), CONCAT('staff_', s.staff_id, '@company.com')) as email,
    s.store_id,
    COALESCE(s.active, 1) as active,
    COALESCE(NULLIF(s.username, ''), CONCAT('user_', s.staff_id)) as username,
    COALESCE(a.address, 'Address not available') as address_line1,
    COALESCE(a.district, 'Unknown') as district,
    COALESCE(c.city, 'Unknown') as city,
    COALESCE(NULLIF(a.postal_code, ''), '00000') as postal_code,
    COALESCE(NULLIF(a.phone, ''), 'Not provided') as phone,
    COALESCE(co.country, 'Unknown') as country
FROM sakila.staff s
LEFT JOIN sakila.address a ON s.address_id = a.address_id
LEFT JOIN sakila.city c ON a.city_id = c.city_id
LEFT JOIN sakila.country co ON c.country_id = co.country_id
WHERE s.staff_id IN (99, 100, 101)
ON DUPLICATE KEY UPDATE
    first_name = VALUES(first_name),
    last_name = VALUES(last_name),
    email = VALUES(email);

-- ================================================================
-- STEP 11: SAMPLE ANALYTICAL QUERIES
-- ================================================================

-- Query 1: Monthly Payment Analysis by Staff
SELECT 
    dd.year,
    dd.month_name,
    ds.first_name,
    ds.last_name,
    SUM(fmp.total_payment_amount) as monthly_total,
    SUM(fmp.rental_count) as total_rentals,
    AVG(fmp.avg_payment_per_rental) as avg_payment
FROM fact_monthly_payment fmp
JOIN dim_date dd ON fmp.payment_date_key = dd.date_key
JOIN dim_staff ds ON fmp.staff_key = ds.staff_key
GROUP BY dd.year, dd.month, ds.staff_key
ORDER BY dd.year, dd.month, monthly_total DESC;

-- Query 2: Film Inventory Utilization by Store
SELECT 
    dst.store_id,
    dst.city,
    df.title,
    df.category_name,
    AVG(fdi.utilization_rate) as avg_utilization,
    AVG(fdi.available_inventory_count) as avg_available,
    AVG(fdi.total_inventory_count) as avg_total
FROM fact_daily_inventory fdi
JOIN dim_store dst ON fdi.store_key = dst.store_key
JOIN dim_film df ON fdi.film_key = df.film_key
JOIN dim_date dd ON fdi.inventory_date_key = dd.date_key
WHERE dd.year = 2005
GROUP BY dst.store_key, df.film_key
HAVING AVG(fdi.utilization_rate) > 50
ORDER BY avg_utilization DESC;

-- Query 3: Top Performing Staff by Quarter
SELECT 
    dd.year,
    dd.quarter,
    ds.first_name,
    ds.last_name,
    SUM(fmp.total_payment_amount) as quarterly_revenue,
    RANK() OVER (PARTITION BY dd.year, dd.quarter ORDER BY SUM(fmp.total_payment_amount) DESC) as revenue_rank
FROM fact_monthly_payment fmp
JOIN dim_date dd ON fmp.payment_date_key = dd.date_key
JOIN dim_staff ds ON fmp.staff_key = ds.staff_key
GROUP BY dd.year, dd.quarter, ds.staff_key
ORDER BY dd.year, dd.quarter, revenue_rank;

-- ================================================================
-- END OF SOLUTION
-- ================================================================

-- Summary of Implementation:
-- 1. Created dimensional model with Star Schema design
-- 2. Implemented comprehensive ETL with data quality handling
-- 3. Added performance optimization through indexing
-- 4. Included data validation and testing procedures
-- 5. Provided sample analytical queries for verification

-- The solution handles:
-- - Redundant data through DISTINCT and ON DUPLICATE KEY UPDATE
-- - Missing values using COALESCE and NULLIF functions
-- - Data type conversions and formatting
-- - Performance optimization through proper indexing
-- - Business logic validation through sample queries