package apex;

import java.math.BigDecimal;

public class Product {
    private final int itemId;
    private final BigDecimal itemPrice;
    private final String itemUPC;
    private final int frequency;

    // Constructor
    public Product(int itemId, BigDecimal itemPrice, String itemUPC, int frequency) {
        this.itemId = itemId;
        this.itemPrice = itemPrice;
        this.itemUPC = itemUPC;
        this.frequency = frequency;
    }

    public int getItemId() {
        return itemId;
    }

    public BigDecimal getItemPrice() {
        return itemPrice;
    }

    public String getItemUPC() {
        return itemUPC;
    }

    public int getFrequency() {
        return frequency;
    }
}
