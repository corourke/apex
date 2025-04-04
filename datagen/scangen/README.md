## Retail Point of Sale Transaction Generator

A data generator written in Java that simulates point-of-sale transactions. It generates transactions and sends them to a Kafka topic. It is multi-threaded and can run at fairly high transaction rates if needed.

The purpose of this program is to simulate point-of-sale (checkout) transactions in large quantities for analytical processing. It is part of a real-world scenario based on a large retailer that has hundreds of stores throughout the US. The data team needed to bring near real-time sales data together for immediate analysis around the holidays in a pricing 'war room' situation.

The program generates and sends cash register 'scans' for random products in the `item_master` table for all the stores in the `retail_stores` table.

Note: the `scripts/` directory is no longer used. It contains shell scripts that were used to upload flat files to Kafka using the Confluent CLI, this version is on branch `batched-scans`.

### Running Instructions

To start:

Make a copy of the `config.properties.example` file to `config.properties` and edit according to your environment.

Be sure to replace API_KEY and API_SECRET in the `config.properties` file with the credentials from your kafka environment. (These are just placeholders, not environment variables.)

Then:

```bash
mvn package
mvn exec:java
```
