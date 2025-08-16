package apex;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.hudi.DataSourceWriteOptions;

import java.sql.Timestamp;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class InventoryDeltaWriter {
        public static void main(String[] args) {
                SparkSession spark = SparkSession.builder()
                                .appName("apex.InventoryDeltaWriter")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                // .enableHiveSupport() // Enabled for Hive integration with saveAsTable
                                .getOrCreate();

                // Parse lookback days argument (default to 1)
                int lookbackDays = (args.length > 0) ? Integer.parseInt(args[0]) : 1;

                // Define table details
                String databaseName = "apex_silver";
                String tableName = "inventory_delta";
                String tablePath = "s3a://onehouse-customer-bucket-7a00bf9c/datalake/apex_silver/inventory_delta";
                String scansPath = "s3a://onehouse-customer-bucket-7a00bf9c/datalake/apex_bronze/retail_scans/v2";

                // Determine date range: process the prior lookbackDays full days (up to
                // yesterday)
                LocalDate endDate = LocalDate.now().minusDays(1); // Yesterday
                LocalDate startDate = endDate.minusDays(lookbackDays - 1); // Adjust to cover the full lookback period

                // Calculate start and end timestamps for filtering (start of startDate to start
                // of today)
                Timestamp startTs = Timestamp.valueOf(startDate.atStartOfDay());
                Timestamp endTs = Timestamp.valueOf(endDate.plusDays(1).atStartOfDay());

                // Read and filter retail_scans for the full days range
                Dataset<Row> scans = spark.read().format("hudi").load(scansPath)
                                .withColumn("scan_ts", to_timestamp(col("scan_datetime")))
                                .filter(col("scan_ts").geq(lit(startTs)).and(col("scan_ts").lt(lit(endTs))))
                                .filter(col("store_id").isNotNull()
                                                .and(col("item_upc").isNotNull().and(col("unit_qty").isNotNull()))); // Basic
                                                                                                                     // validation

                // Aggregate deltas
                Dataset<Row> deltas = scans
                                .withColumn("delta_date", to_date(col("scan_ts")))
                                .groupBy("delta_date", "store_id", "item_upc")
                                .agg(sum("unit_qty").cast(DataTypes.IntegerType).alias("net_quantity_delta"))
                                .withColumn("last_updated", current_timestamp());

                // Write/upsert to Hudi table
                String tableType = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(); // MOR (Merge-On-Read)
                deltas.write()
                                .format("hudi")
                                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "delta_date,store_id,item_upc") // Composite
                                                                                                                        // key
                                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "last_updated") // Latest wins
                                                                                                         // for upserts
                                .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType)
                                .option("hoodie.table.name", tableName)
                                .option("path", tablePath)
                                .mode("append") // Handles insert/upsert
                                .saveAsTable(String.format("%s.%s", databaseName, tableName)); // Save and register in
                                                                                               // Hive metastore

                spark.stop();
        }
}