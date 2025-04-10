// Demonstrate table mutation handling, including clustering, compaction, and cleaning, by inducing
// Postgres table inserts, updates and deletes. This program simulates price fluctuations, 
// the addition of new items, and removal of items in the item_master table. 

package acme;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashSet;

public class PriceUpdater implements Runnable {
    private Properties props = new Properties();
    private Connection DBconnection;
    private Random rand = new Random();
    private static final Logger logger = Logger.getLogger(PriceUpdater.class.getName());

    private Set<String> existingUPCs = new HashSet<>();
    private static final int MIN_DELETABLE_ITEM_ID = 50000;

    public static void main(String[] args) {
        PriceUpdater updater = new PriceUpdater();
        Runtime.getRuntime().addShutdownHook(new Thread(updater::shutdown));
        logger.setLevel(Level.INFO);
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%4$s: %2$s %1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp - %5$s%6$s%n");

        updater.run();
    }

    private void shutdown() {
        logger.log(Level.INFO, "Shutting down...");
        closeConnection();
        // Any cleanup code can go here
    }

    public PriceUpdater() {
        try {
            // Load properties
            props.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load properties file", e);
            System.exit(1);
        }
    }

    public void run() {
        // Register JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to load JDBC driver", e);
            return;
        }

        // The main part of the program, exits on error or when interrupted
        DBconnection = null;
        try {
            DBconnection = createConnection();
            logger.log(Level.INFO, "Connected to the PostgreSQL server successfully.");
            // Main loop
            while (!Thread.currentThread().isInterrupted()) {
                // Check time
                if (isBusinessHours()) {
                    // Check that connection is still valid
                    if (!DBconnection.isValid(5)) { // 5 seconds timeout
                        logger.log(Level.WARNING, "Database connection is no longer valid, reconnecting.");
                        DBconnection = createConnection();
                    }
                    // Do the next set of updates
                    int categoryCode = selectRandomCategory(DBconnection);
                    if (categoryCode != 0) {
                        logger.log(Level.INFO, String.format("Doing a price update for category: %d", categoryCode));
                        updateItemPrices(DBconnection, categoryCode);
                    }
                    // Insert a few new dummy items
                    insertNewItems(DBconnection, categoryCode);

                    // Delete a few items
                    deleteItems(DBconnection);

                    // Sleep for a random time
                    sleepRandomTime();
                } else {
                    Thread.sleep(600000); // Sleep for 10 minutes before checking time again
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "SQL Error: " + e.getMessage());
        } catch (InterruptedException e) {
            // Nothing to do here as the interrupt will be handled by the shutdown hook
        } finally {
            closeConnection();
        }
    }

    // Create a connection to the database, nothin fancy
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + props.getProperty("database.host") + ":" +
                        props.getProperty("database.port") + "/" + props.getProperty("database.name"),
                props.getProperty("database.user"), props.getProperty("database.password"));
    }

    private void closeConnection() {
        logger.log(Level.INFO, "Closing the database connection...");
        try {
            if (DBconnection != null && DBconnection.isValid(5)) {
                DBconnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to close the database connection", e);
        }
    }

    // Select a random category_code from the item_categories table
    private int selectRandomCategory(Connection conn) throws SQLException {
        String sql = "SELECT category_code FROM item_categories ORDER BY RANDOM() LIMIT 1";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            return rs.getInt("category_code");
        }
        return 0;
    }

    // Update the prices of a random set of items in the given category
    private void updateItemPrices(Connection conn, int categoryCode) throws SQLException {
        int updateCount = getRandomIntBetween(20, 60); // Replace: 20 + rand.nextInt(41)
        String sql = "WITH updated AS (SELECT item_id FROM item_master WHERE category_code = ? ORDER BY RANDOM() LIMIT "
                + updateCount + ") " +
                "UPDATE item_master SET base_price = base_price * (0.9 + (0.2 * RANDOM())) FROM updated WHERE item_master.item_id = updated.item_id";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, categoryCode);
        pstmt.executeUpdate();
        logger.log(Level.INFO, String.format("Number of prices updated: %d", updateCount));
    }

    // Insert a few new dummy items in the given category
    private void insertNewItems(Connection conn, int categoryCode) throws SQLException {
        int numNewItems = getRandomIntBetween(5, 20);
        String upc;
        int nextItemId = findNextItemId(conn);
        logger.log(Level.INFO, String.format("Next item_id: %d", nextItemId));
        // Load existing UPCs if the set is empty
        if (existingUPCs.isEmpty()) { // TODO: this is always evaluating to true
            loadExistingUPCs(conn);
        }

        String sql = "INSERT INTO item_master "
                + "(category_code, item_id, base_price, upc_code, case_qty, _frequency) "
                + "VALUES (?, ?, 1.00, ?, 1, 1)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= numNewItems; i++) {
                do {
                    upc = generateUPC();
                } while (existingUPCs.contains(upc));
                existingUPCs.add(upc);

                pstmt.setInt(1, categoryCode);
                pstmt.setInt(2, nextItemId++);
                pstmt.setString(3, upc);
                pstmt.addBatch();
            }
            int[] updateCounts = pstmt.executeBatch();
            logger.log(Level.INFO, String.format("Number of new items inserted: %d", updateCounts.length));
        }
    }

    private void deleteItems(Connection conn) throws SQLException {
        int deleteCount = getRandomIntBetween(5, 10);
        String sql = "DELETE from item_master where item_id in ("
                + " SELECT item_id from item_master WHERE item_id >= ?"
                + " ORDER BY RANDOM() LIMIT ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, MIN_DELETABLE_ITEM_ID);
            pstmt.setInt(2, deleteCount);
            int rowsAffected = pstmt.executeUpdate();
            logger.log(Level.INFO, String.format("Number of items deleted: %d", rowsAffected));
        }
    }

    // Helper function to generate a random 11-digit UPC (without check digit)
    private String generateUPC() {
        StringBuilder sb = new StringBuilder();
        sb.append("2"); // Start with '2' so we can easily spot them
        for (int i = 1; i < 11; i++) { // Notice the loop starts at 1
            sb.append(rand.nextInt(10));
        }
        return sb.toString();
    }

    // Load existing UPCs into memory
    private void loadExistingUPCs(Connection conn) throws SQLException {
        existingUPCs.clear(); // Reset the set before loading
        PreparedStatement pstmt = conn.prepareStatement("SELECT upc_code FROM item_master");
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            existingUPCs.add(rs.getString("upc_code"));
        }
        logger.log(Level.INFO, String.format("Loaded %d UPCs", existingUPCs.size()));
    };

    // Helper function to find the next item_id
    private int findNextItemId(Connection conn) throws SQLException {
        String sql = "SELECT MAX(item_id) max_item_id FROM item_master";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        int maxItemId = MIN_DELETABLE_ITEM_ID;
        if (rs.next()) {
            maxItemId = rs.getInt("max_item_id");
            maxItemId++;
        }
        if (maxItemId < MIN_DELETABLE_ITEM_ID) {
            maxItemId = MIN_DELETABLE_ITEM_ID;
        }
        return maxItemId;
    }

    // Check if it is business hours
    private boolean isBusinessHours() {
        LocalTime now = LocalTime.now();
        boolean isBusinessHours = !(now.isAfter(LocalTime.of(17, 0)) || now.isBefore(LocalTime.of(9, 0)));
        logger.log(Level.INFO, String.format("Business hours? %s", isBusinessHours));
        return isBusinessHours;

    }

    // Sleep for a random time
    private void sleepRandomTime() {
        int sleepTime = getRandomIntBetween(5, 20); // Replace: 5 + rand.nextInt(16)
        logger.log(Level.INFO, String.format("Sleeping for %d minutes", sleepTime));
        try {
            TimeUnit.MINUTES.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Return a random integer between min and max (inclusive)
    private int getRandomIntBetween(int min, int max) {
        if (min > max) {
            throw new IllegalArgumentException("max must be greater than or equal to min");
        }
        return min + rand.nextInt(max - min + 1);
    }

}
