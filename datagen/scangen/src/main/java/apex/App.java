package apex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Data generator. Generates retail scan transactions related to a list of
 * products and stores and sends them to a Kafka topic.
 */

public class App {
    static Connection dbConnection = null;
    static Logger logger = Logger.getLogger(App.class.getName());
    static Properties config = new Properties();

    public static void main(String[] args) {

        // Disable Jansi library as it has a bug that causes the console to hang
        System.setProperty("log4j.skipJansi", "true");

        // Configure file logging
        try {
            FileHandler fileHandler = new FileHandler("scangen.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.SEVERE);
            logger.addHandler(fileHandler);
            logger.setLevel(Level.INFO);
            for (var handler : logger.getParent().getHandlers()) {
                handler.setLevel(Level.INFO);
            }
        } catch (IOException e) {
            System.err.println("Failed to configure file logging: " + e.getMessage());
            System.exit(1);
        }

        // Load properties file
        try {
            config.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            System.out.println("Unable to load configuration file.");
            logger.log(Level.SEVERE, "Configuration file loading failed", e);
            System.exit(1);
        }

        // Connect to the database
        try {
            dbConnection = createConnection();
            logger.log(Level.INFO, "Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database connection failed", e);
            System.exit(1);
        }

        // Load lookup tables from the Postgres database
        List<Product> products = loadProductsFromDB();
        System.out.println("Products read: " + products.size());
        List<Store> stores = loadStoresFromDB();
        System.out.println("Stores read: " + stores.size());
        closeConnection();

        // Initialize Kafka configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
        kafkaProps.put("security.protocol", config.getProperty("security.protocol"));
        kafkaProps.put("sasl.mechanism", config.getProperty("sasl.mechanism"));
        kafkaProps.put("sasl.jaas.config", config.getProperty("sasl.jaas.config"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Make a list of the unique timezones in the stores list
        List<String> timezones = stores.stream().map(Store::getTimezone).distinct().collect(Collectors.toList());

        // Initialize and start the transaction generators
        System.out.println("Starting generator threads...");
        Map<String, Thread> generatorThreads = new HashMap<>();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            generatorThreads.forEach((timezone, generatorThread) -> {
                generatorThread.interrupt(); // Attempt to stop the generator thread
            });
        }));

        for (String timezone : timezones) {
            TransactionGenerator generator = new TransactionGenerator(config, kafkaProps, products, stores, timezone,
                    logger);
            Thread generatorThread = new Thread(generator);
            generatorThreads.put(timezone, generatorThread);
            generatorThread.start();
            // Delay for 7 seconds to keep reporting from interleaving
            try {
                Thread.sleep(7000);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Thread sleep interrupted", e);
            }
        }
    }

    // Create a connection to the database, nothin fancy
    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + config.getProperty("database.host") + ":" +
                        config.getProperty("database.port") + "/" + config.getProperty("database.name"),
                config.getProperty("database.user"),
                config.getProperty("database.password"));
    }

    private static void closeConnection() {
        System.out.println("Closing the database connection...");
        try {
            if (dbConnection != null && dbConnection.isValid(5)) {
                dbConnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to close the database connection", e);
        }
    }

    // Define a functional interface that allows SQLException
    @FunctionalInterface
    interface ResultSetMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    // Generic method to load data from the DB using a query and a mapper
    private static <T> List<T> loadFromDB(String query, ResultSetMapper<T> mapper) {
        List<T> result = new ArrayList<>();
        try (Statement stmt = dbConnection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                result.add(mapper.map(rs));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error loading data from DB: " + e.getMessage());
            System.exit(1);
        }
        return result;
    }

    // Load products from DB
    private static List<Product> loadProductsFromDB() {
        String productQuery = "SELECT ITEM_ID, BASE_PRICE, UPC_CODE, _FREQUENCY FROM retail.item_master WHERE ITEM_ID < 50000";
        return loadFromDB(productQuery, rs -> new Product(
                rs.getInt("ITEM_ID"),
                rs.getBigDecimal("BASE_PRICE"),
                rs.getString("UPC_CODE"),
                rs.getInt("_FREQUENCY")));
    }

    // Load stores from DB
    private static List<Store> loadStoresFromDB() {
        String storeQuery = "SELECT store_id, state, timezone FROM retail.stores";
        return loadFromDB(storeQuery, rs -> new Store(
                rs.getInt("store_id"),
                rs.getString("state"),
                rs.getString("timezone")));
    }
}