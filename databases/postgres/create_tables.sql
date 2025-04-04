SET SEARCH_PATH TO retail;

-- Table 1: Categories
CREATE TABLE item_categories (
    category_code INTEGER PRIMARY KEY, 
    category_name VARCHAR(255), 
    category_description VARCHAR(255),
    _avg_price NUMERIC, -- Used for data generation
    _frequency NUMERIC  -- Used for data generation
);

-- Table 2: Item Master
CREATE TABLE item_master (
    category_code INTEGER, 
    item_id INTEGER PRIMARY KEY,
    base_price DECIMAL(10,2), 
    upc_code VARCHAR(15) UNIQUE, 
    case_qty INTEGER,
    _frequency INTEGER -- Used for data generation
);
CREATE INDEX idx_upc_code ON item_master (upc_code);
ALTER TABLE IF EXISTS item_master
    ADD CONSTRAINT fk_item_category FOREIGN KEY (category_code)
    REFERENCES item_categories (category_code) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;

-- Table 3: Stores
CREATE TABLE stores (
    store_id INTEGER PRIMARY KEY, 
    address VARCHAR(255),
    city VARCHAR (80),
    state VARCHAR (2),
    zipcode VARCHAR(10),
    longitude NUMERIC,
    latitude NUMERIC, 
    region VARCHAR(32),
    timezone VARCHAR(32)
);

