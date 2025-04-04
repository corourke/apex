package apex;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.math.BigDecimal;
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

        processTransaction(transaction);
      }
    }

    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    System.out.println(now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) +
        " Region: " + timezone + " at: " + localTime + " produced " + inProcessTransactionCount + " transactions");
  }

  private void processTransaction(Transaction transaction) {
    producer.send(new ProducerRecord<>(kafkaTopic, transaction.toJSON()), (metadata, exception) -> {
      if (exception != null) {
        logger.log(Level.SEVERE, "Failed to send transaction to Kafka, timezone: " + timezone, exception);
        // Optionally, still throw to stop the thread if desired
        // throw new RuntimeException(exception);
      }
    });

    if ((random.nextInt(200) + 1) == 1) {
      int delaySeconds = 60 + random.nextInt(121);
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
      multiplier = 0.5;
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
}