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


-- ================================================================
-- DIMENSION TABLES CREATION
-- ================================================================

-- 1. Date Dimension Table
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE,
    day INT,
    month INT,
    month_name VARCHAR(20),
    quarter VARCHAR(10),
    year INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- 2. Staff Dimension Table
CREATE TABLE dim_staff (
    staff_key INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    store_name VARCHAR(100),
    staff_status VARCHAR(20),
    address VARCHAR(255)
);


-- 3. Rental Dimension Table
CREATE TABLE dim_rental (
    rental_key INT PRIMARY KEY AUTO_INCREMENT,
    rental_date DATE,
    return_date DATE,
    customer_name VARCHAR(100),
    film_title VARCHAR(255),
    rental_status VARCHAR(50)
);


-- 4. Film Dimension Table
CREATE TABLE dim_film (
    film_key INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255),
    description TEXT,
    release_year INT,
    language VARCHAR(50),
    rating VARCHAR(10),
    special_features TEXT,
    category VARCHAR(50)
);


-- 5. Store Dimension Table
CREATE TABLE dim_store (
    store_key INT PRIMARY KEY AUTO_INCREMENT,
    store_name VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    manager_name VARCHAR(100)
);


-- ================================================================
-- FACT TABLES CREATION
-- ================================================================

-- Fact Table 1: Monthly Payment per Staff per Rent
CREATE TABLE fact_monthly_payment (
    payment_key INT PRIMARY KEY AUTO_INCREMENT,
    rental_key INT,
    staff_key INT,
    date_key INT,
    amount DECIMAL(10,2),
    month_name VARCHAR(20),
    FOREIGN KEY (rental_key) REFERENCES dim_rental(rental_key),
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);


-- Fact Table 2: Daily Inventory per Film per Store
CREATE TABLE fact_daily_inventory (
    inventory_key INT PRIMARY KEY AUTO_INCREMENT,
    date_key INT,
    film_key INT,
    store_key INT,
    available_qty INT,
    rented_qty INT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (film_key) REFERENCES dim_film(film_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
);

