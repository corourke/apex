# Apex demo scenario

This is a demo scenario for data engineering products. It is applicable to streaming data platforms, ETL, data lakehouses, data warehouses, analytics, and machine learning platforms.

The 'Apex' repo is meant to satisfy a need for a simple, realistic, cohesive and continuously running data engineering scenario that can be used to craft curated demonstrations for prospects, analysts, press, and trade shows. I have also found it valuable for creating clean and attractive UI for making website screenshots and demonstration videos.

The scenario is based on a real-world scenario I encountered at a major retailer. They wanted to gather all retail point-of-sale (POS) data across thousands of stores in near-real time for instant analysis, particularly around the competitive holiday period.

The repo includes scheams, starter data, sample queries, and various configurations needed to emulate a data lakehouse pipeline. Sensible and realistic data is accomplished with data generators that combine simulation, randomness, and real-world sampling. A full build-out will rely on AWS, Confluent, Postgres, Databricks, Snowflake, dbt and other components.

#### Block diagram

<picture>
<img alt="Architecture diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture.png?raw=true">
</picture>

#### Tables

There are four main tables used by the demo. The tables are relational, with both low- and high-cardinality selectors.

1. Products are split into 32 different product categories that are contained in the `item_categories` table. This table is static.
2. The `stores` table represents 1000 stores across different timezones (that are used to generate activity based on the time of day) and for now remains static.
3. The `item_master` table starts off with about 32000 unique products. There is a continuously running data generator that creates new items, deletes old items, and updates prices. This allows us to show inserts, updates and deletes but at a relatively low volume. The data is updated in a Postgres database and ingested via CDC.
4. The `scans` table represents the point-of-sale register scans, and is fed by a continuously running high-velocity data generator that simulates activity at the 1000 stores and feeds the Kafka stream that is ingested into the data lake.

<picture>
<img alt="Table ER diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables.png?raw=true">
</picture>

I'm working on a comprehensive and standalone setup guide. In the meantime, the <a href="https://github.com/corourke/acme/blob/1d030873fe3b34fdf62fbfecf17a6db049c0b7f1/doc/Onehouse_Postgres_CDC_Guide_2401.pdf">Onehouse PostgreSQL CDC Getting Started Guide will have to do.</a>

### Notes

1. The item master is random and simple. I'd like to improve it. A much better item master is available, for a price, at: https://crawlfeeds.com/datasets/target-products-dataset

## Motivation

This is the third iteration of this demo that I've built for different data engineering products. These are the requirements for a good demo system IMO:

- It must do a good job of illustrating all of the product functionlity.
  - This generally means that we want to see current activity including speeds and feeds, populated chars, metrics, history, etc.
  - This means we need to continuously generate and ingest data.
- It must focus the viewer on product strengths and steer clear of product weaknesses.
  - This means that the content is curated, simple and clean.
  - It presents a consistent and coherent scenario that does not burden or distract viewers.
  - It provides sensible opportunities for analytics, stream processing, and machine learning.
