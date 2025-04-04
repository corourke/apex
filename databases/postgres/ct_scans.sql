-- Point-of-sale Scans
-- This table is actually in the datalake, but including it for reference
CREATE TABLE scans (
    scan_id VARCHAR(48) PRIMARY KEY,
    store_id INTEGER,
    scan_datetime TIMESTAMP,
    upc_code VARCHAR(15),
    unit_qty INTEGER,
    unit_price DECIMAL(10,2)
);
ALTER TABLE IF EXISTS scans
    ADD CONSTRAINT fk_store_id FOREIGN KEY (store_id)
    REFERENCES stores (store_id) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;
ALTER TABLE IF EXISTS scans
    ADD CONSTRAINT fk_upc_code FOREIGN KEY (upc_code)
    REFERENCES item_master (upc_code) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;
