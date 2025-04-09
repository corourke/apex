package apex;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.net.URI;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;
import java.util.logging.Level;

import apex.utils.WeightedRandomListSelector;

public class TransactionGenerator implements Runnable {
  private Random random = new Random();
  private KafkaProducer<String, String> producer;
  private String kafkaTopic;
  private Logger logger; // Add logger
  private final String webSocketUri; // e.g., "ws://localhost:8080"
  private WebSocketClient webSocketClient;

  private List<Product> products;
  private List<Store> stores;
  private WeightedRandomListSelector<Product> productWeights;

  String timezone = null;
  boolean running = false;
  LocalDateTime localDateTime;
  LocalTime localTime;
  int offsetHours;
  int transactionOutput;

  private static class ScheduledVoidTransaction {
    Transaction original;
    LocalDateTime scheduledTime;

    ScheduledVoidTransaction(Transaction original, LocalDateTime scheduledTime) {
      this.original = original;
      this.scheduledTime = scheduledTime;
    }
  }

  private List<ScheduledVoidTransaction> pendingVoids = Collections.synchronizedList(new ArrayList<>());

  public TransactionGenerator(Properties config, Properties kafkaProps, List<Product> products, List<Store> stores,
      String timezone, Logger logger) {
    this.products = products;
    this.productWeights = new WeightedRandomListSelector<>();
    this.productWeights.preComputeCumulativeWeights(products);

    kafkaProps.put("client.id", timezone + "-generator");
    kafkaProps.put("delivery.timeout.ms", "300000"); // 5 minutes
    this.producer = new KafkaProducer<>(kafkaProps);
    this.kafkaTopic = config.getProperty("topic");

    this.stores = stores.stream()
        .filter(store -> store.getTimezone().equals(timezone))
        .collect(Collectors.toList());
    this.timezone = timezone;
    this.logger = logger; // Initialize logger

    this.webSocketUri = config.getProperty("webui.socket.uri");
    connectToWebSocket();

    Pattern pattern = Pattern.compile("GMT([+-]\\d{2})");
    Matcher matcher = pattern.matcher(timezone);
    if (matcher.find()) {
      this.offsetHours = Integer.parseInt(matcher.group(1));
    }
    System.out.println("Generator starting, timezone: " + timezone + " offset: " + offsetHours);
  }

  @Override
  public void run() {
    running = true;
    while (running) {
      try {
        generateTransactions();
        processPendingVoids();
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        System.out.println("Generator interrupted, timezone: " + timezone);
        running = false;
      }
    }
    producer.close();
    System.out.println("Generator exiting, timezone: " + timezone);
  }

  public void stopRunning() {
    running = false;
  }

  private void generateTransactions() {
    localDateTime = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    localTime = localDateTime.toLocalTime();

    int inProcessTransactionCount = 0;
    int inProcessDataVolume = 0;
    for (Store store : stores) {
      int transactionTarget = getTransactionTarget();
      inProcessTransactionCount += transactionTarget;

      for (int i = 0; i < transactionTarget; i++) {
        Product product = productWeights.selectNextWeightedItem(products);
        BigDecimal price = product.getItemPrice();
        int quantity = random.nextInt(10) < 9 ? 1 : 2 + random.nextInt(3);
        int randomSeconds = random.nextInt(60);
        LocalDateTime randomizedDateTime = localDateTime.plusSeconds(randomSeconds);

        Transaction transaction = new Transaction(
            UUID.randomUUID(),
            store.getStoreId(),
            randomizedDateTime,
            product.getItemUPC(),
            quantity,
            price);

        inProcessDataVolume += transaction.toJSON().length();
        processTransaction(transaction);
      }
    }

    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    var timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    System.out.println(timestamp +
        " Timezone: " + timezone + " at: " + localTime + " produced " + inProcessTransactionCount + " transactions");
    sendActivitySummary(timestamp, inProcessTransactionCount, inProcessDataVolume);
  }

  private void processTransaction(Transaction transaction) {
    producer.send(new ProducerRecord<>(kafkaTopic, transaction.toJSON()), (metadata, exception) -> {
      if (exception != null) {
        logger.log(Level.SEVERE, "Failed to send transaction to Kafka, timezone: " + timezone, exception);
        // Optionally, still throw to stop the thread if desired
        // throw new RuntimeException(exception);
      }
    });

    // One in 200 scans will be voided in the next 30 to 90 seconds
    if ((random.nextInt(200) + 1) == 1) {
      int delaySeconds = 30 + random.nextInt(61);
      LocalDateTime scheduledTime = LocalDateTime.now(ZoneId.of("UTC"))
          .plusHours(offsetHours).plusSeconds(delaySeconds);
      pendingVoids.add(new ScheduledVoidTransaction(transaction, scheduledTime));
    }
  }

  private void processPendingVoids() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    List<ScheduledVoidTransaction> dueVoids = new ArrayList<>();

    synchronized (pendingVoids) {
      for (ScheduledVoidTransaction svt : pendingVoids) {
        if (!svt.scheduledTime.isAfter(now)) {
          dueVoids.add(svt);
        }
      }
      pendingVoids.removeAll(dueVoids);
    }

    if (dueVoids.size() == 0)
      return;

    for (ScheduledVoidTransaction svt : dueVoids) {
      Transaction voidTransaction = new Transaction(
          svt.original.getScanId(),
          svt.original.getStoreId(),
          svt.original.getScanDatetime(),
          svt.original.getItemUPC(),
          0,
          svt.original.getUnitPrice());
      processTransaction(voidTransaction);
    }
    System.out.println("    Voided " + dueVoids.size() + " including: " + dueVoids.get(0).original.getScanId()
        + " pending: " + pendingVoids.size());
  }

  private int getTransactionTarget() {
    int baseTransactions = calculateBaseTransactions(localTime);
    double randomFactor = 0.8 + (1.2 - 0.8) * random.nextDouble();
    baseTransactions = Math.max((int) (baseTransactions * randomFactor), 0);
    return adjustTransactionsForTimezone(baseTransactions, timezone);
  }

  private int calculateBaseTransactions(LocalTime time) {
    int hour = time.getHour();
    double multiplier;
    final int CHECKSTANDS = 10;
    final int SCANS_PER_HOUR = 800;

    if (hour >= 10 && hour < 11) {
      multiplier = 0.12;
    } else if (hour >= 11 && hour < 13) {
      multiplier = 1.0;
    } else if (hour >= 13 && hour < 16) {
      multiplier = 0.25;
    } else if (hour >= 16 && hour < 19) {
      multiplier = 0.75;
    } else if (hour >= 19 && hour < 22) {
      multiplier = 0.17;
    } else {
      return 0;
    }
    return (int) Math.floor(multiplier * ((CHECKSTANDS * SCANS_PER_HOUR) / 60.0));
  }

  private int adjustTransactionsForTimezone(int baseTransactions, String timezone) {
    int adjustedTransactions;
    if ("HST (GMT-10)".equals(timezone) || "AKST (GMT-09)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.4);
    } else if ("MST (GMT-07)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.8);
    } else {
      adjustedTransactions = baseTransactions;
    }
    return Math.max(adjustedTransactions, 0);
  }

  // Connect to the Web UI's WebSocket server
  private void connectToWebSocket() {
    logger.log(Level.INFO, "Web socket URI: " + webSocketUri);
    try {
      webSocketClient = new WebSocketClient(new URI(webSocketUri)) {
        @Override
        public void onOpen(ServerHandshake handshakedata) {
          logger.log(Level.INFO, "Connected to Web UI for " + timezone);
        }

        @Override
        public void onMessage(String message) {
          /* Handle shutdown if needed */ }

        @Override
        public void onClose(int code, String reason, boolean remote) {
        }

        @Override
        public void onError(Exception ex) {
          logger.log(Level.WARNING, ex.toString());
        }
      };
      webSocketClient.connect();
    } catch (Exception e) {
      logger.log(Level.WARNING, "WebSocket connection failed: " + e.getMessage());
    }
  }

  // Send activity level summary to the Web UI
  // {'type': 'scans', 'timezone': 'MST (GMT-07)',
  // 'batchCount': 122, 'timestamp': '2025-04-07 14:42:59', 'dataVolume': 10024,
  // 'currentTime': '14:42:59'}
  private void sendActivitySummary(String timestamp, int transactionCount, int dataVolume) {
    if (webSocketClient != null && webSocketClient.isOpen()) {
      JSONObject summary = new JSONObject();
      summary.put("type", "scans");
      summary.put("timezone", timezone);
      summary.put("batchCount", transactionCount);
      summary.put("timestamp", timestamp);
      summary.put("dataVolume", dataVolume);
      summary.put("localTime", localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      webSocketClient.send(summary.toString());
    }
  }

}