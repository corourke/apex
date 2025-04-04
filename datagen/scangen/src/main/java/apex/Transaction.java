package apex;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.json.JSONObject;

public class Transaction {
  private final UUID scanId;
  private final int storeId;
  private final LocalDateTime scanDatetime;
  private final String itemUPC;
  private final int unitQty;
  private final BigDecimal unitPrice;

  // Constructor for Transaction
  public Transaction(UUID scanId, int storeId, LocalDateTime scanDatetime, String itemUPC, int unitQty,
      BigDecimal unitPrice) {
    this.scanId = scanId;
    this.storeId = storeId;
    this.scanDatetime = scanDatetime;
    this.itemUPC = itemUPC;
    this.unitQty = unitQty;
    this.unitPrice = unitPrice;
  }

  // Method to convert Transaction to JSON String
  public String toJSON() {
    JSONObject json = new JSONObject();
    json.put("scan_id", this.getScanId().toString());
    json.put("store_id", this.getStoreId());
    json.put("scan_datetime", this.getScanDatetime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    json.put("item_upc", this.getItemUPC());
    json.put("unit_qty", this.getUnitQty());
    json.put("unit_price", this.getUnitPrice());
    return json.toString();
  }

  public UUID getScanId() {
    return scanId;
  }

  public int getStoreId() {
    return storeId;
  }

  public LocalDateTime getScanDatetime() {
    return scanDatetime;
  }

  public String getItemUPC() {
    return itemUPC;
  }

  public int getUnitQty() {
    return unitQty;
  }

  public BigDecimal getUnitPrice() {
    return unitPrice;
  }

}
