package apex;

// Import Kafka producer packages
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;

import apex.utils.WeightedRandomListSelector;

/**
 * Generate transactions for a list of stores in a given timezone
 */
public class TransactionGenerator implements Runnable {
  // Setup variables
  private Random random = new Random();
  private KafkaProducer<String, String> producer;
  private String kafkaTopic;

  private List<Product> products;
  private List<Store> stores;
  private WeightedRandomListSelector<Product> productWeights;

  // Status variables
  String timezone = null;
  boolean running = false;
  LocalDateTime localDateTime;
  LocalTime localTime;
  int offsetHours;
  int transactionOutput;

  // New fields for void logic
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
      String timezone) {
    this.products = products;
    this.productWeights = new WeightedRandomListSelector<>();
    this.productWeights.preComputeCumulativeWeights(products);

    kafkaProps.put("client.id", timezone + "-generator");
    this.producer = new KafkaProducer<>(kafkaProps);
    this.kafkaTopic = config.getProperty("topic");

    // make a list of stores in this timezone
    this.stores = stores.stream()
        .filter(store -> store.getTimezone().equals(timezone))
        .collect(Collectors.toList());
    this.timezone = timezone;

    // Extract the offset hours from the timezone string like "PST (GMT-08)"
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
        processPendingVoids(); // Process any due voids before generating new transactions.
        Thread.sleep(60000); // Sleep for 1 minute
      } catch (InterruptedException e) {
        System.out.println("Generator interrupted, timezone: " + timezone);
        running = false;
      }
    }
    // Just in case, never executes
    producer.close();
    System.out.println("Generator exiting, timezone: " + timezone);
  }

  public void stopRunning() {
    running = false;
  }

  private void generateTransactions() {
    localDateTime = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    localTime = localDateTime.toLocalTime();

    // For each store, figure out how many transactions to generate
    int inProcessTransactionCount = 0;
    for (Store store : stores) {
      int transactionTarget = getTransactionTarget();
      inProcessTransactionCount += transactionTarget;

      for (int i = 0; i < transactionTarget; i++) {
        Product product = productWeights.selectNextWeightedItem(products);
        BigDecimal price = product.getItemPrice();
        int quantity = random.nextInt(10) < 9 ? 1 : 2 + random.nextInt(3); // Mostly 1, occasionally 2-4
        // Randomize the transaction time within the minute
        int randomSeconds = random.nextInt(60);
        LocalDateTime randomizedDateTime = localDateTime.plusSeconds(randomSeconds);

        Transaction transaction = new Transaction(
            UUID.randomUUID(),
            store.getStoreId(),
            randomizedDateTime,
            product.getItemUPC(),
            quantity,
            price);

        processTransaction(transaction);
      }
    }

    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    System.out.println(now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) +
        " Region: " + timezone + " at: " + localTime + " produced " + inProcessTransactionCount + " transactions");

  }

  private void processTransaction(Transaction transaction) {
    // Send each transaction to Kafka
    producer.send(new ProducerRecord<>(kafkaTopic, transaction.toJSON()), (metadata, exception) -> {
      if (exception != null) {
        // Handle potential send error
        exception.printStackTrace();
        throw new RuntimeException(exception);
      }
    });

    // Sometimes void a transaction
    if ((random.nextInt(200) + 1) == 1) { // 1 in 200 chance.
      int delaySeconds = 60 + random.nextInt(121); // Delay between 60-180 seconds.
      LocalDateTime scheduledTime = LocalDateTime.now(ZoneId.of("UTC"))
          .plusHours(offsetHours).plusSeconds(delaySeconds);
      pendingVoids.add(new ScheduledVoidTransaction(transaction, scheduledTime));
    }
  }

  private void processPendingVoids() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    List<ScheduledVoidTransaction> dueVoids = new ArrayList<>();

    // Collect due voids and remove them from the pending list within a synchronized
    // block
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

    // Process the due void transactions outside the synchronized block
    for (ScheduledVoidTransaction svt : dueVoids) {
      Transaction voidTransaction = new Transaction(
          svt.original.getScanId(),
          svt.original.getStoreId(),
          svt.original.getScanDatetime(),
          svt.original.getItemUPC(),
          0, // void quantity
          svt.original.getUnitPrice());
      processTransaction(voidTransaction);
      // System.out.println(" Voiding transaction: " + voidTransaction.getScanId());
    }
    System.out.println("    Voided " + dueVoids.size() + " including: " + dueVoids.get(0).original.getScanId()
        + " pending: " + pendingVoids.size());
  }

  // Calculate the number of transactions to generate based on the
  // time of day, the area of the country, and a random factor
  private int getTransactionTarget() {
    int baseTransactions = calculateBaseTransactions(localTime);
    double randomFactor = 0.8 + (1.2 - 0.8) * random.nextDouble();
    baseTransactions = Math.max((int) (baseTransactions * randomFactor), 0);
    return adjustTransactionsForTimezone(baseTransactions, timezone);
  }

  // Determine base number of transactions based on time of day
  private int calculateBaseTransactions(LocalTime time) {
    int hour = time.getHour();
    double multiplier;
    final int CHECKSTANDS = 10; // Checkstands open at busiest stores
    final int SCANS_PER_HOUR = 800; // Industry ranges from 600 to 1200 scans per hour

    // Scale down scans based on time of day
    if (hour >= 10 && hour < 11) {
      multiplier = 0.12;
    } else if (hour >= 11 && hour < 13) {
      multiplier = 1.0;
    } else if (hour >= 13 && hour < 16) {
      multiplier = 0.25;
    } else if (hour >= 16 && hour < 19) {
      multiplier = 0.5;
    } else if (hour >= 19 && hour < 22) {
      multiplier = 0.17;
    } else {
      return 0; // Store closed or not opened yet
    }
    return (int) Math.floor(multiplier * ((CHECKSTANDS * SCANS_PER_HOUR) / 60.0));
  }

  private int adjustTransactionsForTimezone(int baseTransactions, String timezone) {
    // Adjust number of transactions based on store timezone, whole numbers only
    int adjustedTransactions;
    if ("HST (GMT-10)".equals(timezone) || "AKST (GMT-09)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.4);
    } else if ("MST (GMT-07)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.8);
    } else {
      adjustedTransactions = baseTransactions; // Assumes EST-5 or similar traffic without further adjustment
    }
    // Ensure the result is not less than 0
    return Math.max(adjustedTransactions, 0);
  }

}
