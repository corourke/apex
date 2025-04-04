package apex;

public class Store {
    private final int storeId;
    private final String state;
    private final String timezone;

    // Constructor
    public Store(int storeId, String state, String timezone) {
        this.storeId = storeId;
        this.state = state;
        this.timezone = timezone;
    }

    public int getStoreId() {
        return storeId;
    }

    public String getState() {
        return state;
    }

    public String getTimezone() {
        return timezone;
    }

}
