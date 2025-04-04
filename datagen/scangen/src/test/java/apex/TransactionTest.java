package apex;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionTest {

  @Test
  public void testToJSONMatchesAvroSchema() throws Exception {
    // Create a new Transaction using the constructor
    UUID scanId = UUID.randomUUID();
    int storeId = 123;
    LocalDateTime scanDatetime = LocalDateTime.now();
    String itemUPC = "012345678905";
    int unitQty = 2;
    BigDecimal unitPrice = new BigDecimal("19.99");

    Transaction transaction = new Transaction(scanId, storeId, scanDatetime, itemUPC, unitQty, unitPrice);

    // Get the JSON output from toJSON()
    String jsonString = transaction.toJSON();
    System.out.println(jsonString);

    // Load Avro schema from file in project root
    File schemaFile = new File("confluent/scans-values.avsc");
    Schema schema = new Schema.Parser().parse(schemaFile);

    // Parse JSON and populate GenericRecord
    JSONObject jsonObject = new JSONObject(jsonString);
    GenericRecord record = new GenericData.Record(schema);
    record.put("scan_id", jsonObject.getString("scan_id"));
    record.put("store_id", jsonObject.getInt("store_id"));
    record.put("scan_datetime", jsonObject.getString("scan_datetime"));
    record.put("item_upc", jsonObject.getString("item_upc"));
    record.put("unit_qty", jsonObject.getInt("unit_qty"));
    // record.put("unit_price", jsonObject.getString("unit_price"));
    // record.put("unit_price", jsonObject.getBigDecimal("unit_price"));
    // Convert unit_price string to Avro decimal bytes
    // BigDecimal price = new BigDecimal(jsonObject.getString("unit_price"));
    BigDecimal price = jsonObject.getBigDecimal("unit_price");
    ByteBuffer priceBytes = ByteBuffer.wrap(price.unscaledValue().toByteArray());
    record.put("unit_price", priceBytes);

    // Validate the JSON output against the schema
    assertTrue(GenericData.get().validate(schema, record), "toJSON() output does not match Avro schema");
  }
}