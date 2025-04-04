package apex.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

public class WeightedRandomListSelectorTest {
  class TestItem {
    private String name;
    private int frequency;

    public TestItem(String name, int frequency) {
      this.name = name;
      this.frequency = frequency;
    }

    public String getName() {
      return name;
    }

    public int getFrequency() {
      return frequency;
    }
  }

  @Test
  public void testDistribution() {
    // This is a test List of items with different frequencies
    class TestItemList extends ArrayList<TestItem> {
      public TestItem getByName(String name) {
        for (TestItem item : this) {
          if (item.getName().equals(name)) {
            return item;
          }
        }
        return null; // Handle case when item is not found
      }
    }

    TestItemList items = new TestItemList();
    items.add(new TestItem("High", 60));
    items.add(new TestItem("Medium", 30));
    items.add(new TestItem("Low", 10));

    // This is how you set up the WeightedRandomListSelector
    WeightedRandomListSelector<TestItem> selector = new WeightedRandomListSelector<>();
    selector.preComputeCumulativeWeights(items);

    // Run the test
    Map<String, Integer> counts = new HashMap<>();
    int samples = 1000;
    for (int i = 0; i < samples; i++) {
      // This is how you call the WeightedRandomListSelector
      TestItem item = selector.selectNextWeightedItem(items);
      counts.put(item.getName(), counts.getOrDefault(item.getName(), 0) + 1);
    }

    // Assert the distribution is approximately right based on the frequency
    assertTrue(counts.get("High") > counts.get("Medium"));
    assertTrue(counts.get("Medium") > counts.get("Low"));

    // Print out the counts for manual inspection
    System.out.println(counts);
    // As a more exact test, calculate the expected ratios and compare them to the
    // actual ratios in the list
    // caluclate the sum of the frequencies in the items list
    int sum = items.stream().mapToInt(TestItem::getFrequency).sum();
    double highRatio = items.getByName("High").getFrequency() / (double) sum;
    double mediumRatio = items.getByName("Medium").getFrequency() / (double) sum;
    double lowRatio = items.getByName("Low").getFrequency() / (double) sum;

    double epsilon = 0.05; // 5% error allowed
    assertTrue(Math.abs(counts.get("High") / (double) samples - highRatio) < epsilon);
    assertTrue(Math.abs(counts.get("Medium") / (double) samples - mediumRatio) < epsilon);
    assertTrue(Math.abs(counts.get("Low") / (double) samples - lowRatio) < epsilon);

  }
}
