SET SEARCH_PATH TO retail;

\copy item_categories from '../seed-data/item_categories.csv' DELIMITER ',' CSV Header;

\copy item_master from '../seed-data/item_master.csv' DELIMITER ',' CSV Header;

\copy stores  from '../seed-data/retail_stores.csv' DELIMITER ',' CSV Header;