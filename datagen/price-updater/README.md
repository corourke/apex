## Random transaction generator

Simulates transations including price fluctuations, the addition of new items, and removal of old items
within a PostgreSQL database (specifically the `item_master` table defined elsewhere in this project).

The purpose is to add live activity to the demo, and showcase Hudi table mutation handling, including clustering, compaction, and cleaning, by inducing Postgres table inserts, updates and deletes.

The updates simulate the real-time price changes a large retailer might do around the holidays.

Items that are added or removed have an item_id greater than or equal to 50000 in order to preserve referential integrity for the scan records.

> [!NOTE]
>
> Deletes on an item master table are not very realistic. I'd like to add an additional table or two, perhaps a promotions table, or silver tables that could better show mutations.



### Setup

Create the config.properties file:

```
database.host=acme-db.cc64n2qunrtu.us-west-2.rds.amazonaws.com
database.port=5432
database.user=postgres
database.name=acme
database.password=<postgres_user_password>
```

Make the program with `./make.sh`
Start the process using `./start.sh`

### Program functions and logic

1. Reads the database connection information from a properties file.
2. Listens for an OS interrupt to release resources and shutdown gracefully.
3. Connects to the Postgres database, periodically checks the connection, and reconnects if it is disconnected.
4. Reads the `item_categories` table to select a random `category_code` to update.
5. Randomly modifies the price for a subset of items within the chosen category. Prices can increase or decrease by up to 10%.
6. Reads the `item_master` table to find the next item_id to use for new items.
7. Reads the `item_master` table to find existing UPC codes to avoid generating duplicates.
8. Randomly inserts a small number of new items. Generated items have `item_id` > 50000. Ensures that the new UPC codes are unique.
9. Deletes between 5 and 15 randomly selected items with `item_id` > 50000
10. Sleeps for a random period of time. Does not make updates at night.

### Potential enhancements

- **Configurable:** It might be useful to make this program more configurable,
  to change data volumes and sleep times, and to handle more varied
  demo data tasks as opposed to being a single-purpose program.
