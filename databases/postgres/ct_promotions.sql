-- Set up Promotions table (from random selection)
-- This table is not currently being used in the demo
SET
  SEARCH_PATH TO retail;

-- TABLE: retail.promotions
DROP TABLE IF EXISTS retail.promotions;

CREATE TABLE
  IF NOT EXISTS retail.promotions (
    promotion_id serial PRIMARY KEY,
    promotion_name VARCHAR(100),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    discount_type VARCHAR(10) NOT NULL, -- 'PCT_OFF', 'AMT_OFF', 'NEW_PRICE'
    discount_value NUMERIC(10, 2) NOT NULL, -- percentage off, amount off, or new price
    region VARCHAR(32), -- If NULL, applies to all regions
    category_code INTEGER, -- If NULL, applies to all categories
    item_id INTEGER, -- if null, applies to all items
    conversions INTEGER DEFAULT 0
  );

ALTER TABLE IF EXISTS retail.promotions OWNER TO cdc_user;

GRANT ALL ON TABLE retail.promotions TO cdc_user;

-- create a few sample records
INSERT INTO
  retail.promotions (
    promotion_name,
    start_date,
    end_date,
    discount_type,
    discount_value,
    category_code
  )
SELECT
  CONCAT ('Save on ', category_name) AS promotion_name,
  NOW () AS start_date,
  NOW () + INTERVAL '7 days' AS end_date,
  'PCT_OFF' AS discount_type,
  FLOOR(POWER(RANDOM (), 3) * (10 -5) + 5) AS discount_value,
  category_code
FROM
  item_categories TABLESAMPLE bernoulli (30);

-- create a few sample records (SQL)
INSERT INTO retail.promotions (promotion_name,start_date,end_date,discount_type,discount_value,region,category_code,item_id,conversions) VALUES
	 ('Save on Groceries','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20100,NULL,0),
	 ('Save on Paper Goods','2024-07-15','2024-07-22','PCT_OFF',8.00,NULL,20190,NULL,0),
	 ('Save on Clothing, Men’s','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20200,NULL,0),
	 ('Save on Housewares','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20280,NULL,0),
	 ('Save on Appliances','2024-07-15','2024-07-22','PCT_OFF',9.00,NULL,20300,NULL,0),
	 ('Save on Audio','2024-07-15','2024-07-22','PCT_OFF',8.00,NULL,20330,NULL,0),
	 ('Save on Computers','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20370,NULL,0),
	 ('Save on Jewelry','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20400,NULL,0),
	 ('Save on Hardware and Tools','2024-07-15','2024-07-22','PCT_OFF',5.00,NULL,20600,NULL,0),
	 ('Save on Lumber','2024-07-15','2024-07-22','PCT_OFF',6.00,NULL,20640,NULL,0);
