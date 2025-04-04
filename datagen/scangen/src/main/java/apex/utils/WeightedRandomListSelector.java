package apex.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WeightedRandomListSelector<T> {
    private List<Integer> cumulativeWeights = new ArrayList<>();
    private int totalWeight;
    private Random random = new Random();

    public void preComputeCumulativeWeights(List<T> items) {
        cumulativeWeights.clear();
        totalWeight = 0;
        for (T item : items) {
            try {
                Method getFrequencyMethod = item.getClass().getMethod("getFrequency");
                int frequency = (Integer) getFrequencyMethod.invoke(item);
                totalWeight += frequency;
                cumulativeWeights.add(totalWeight);
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke getFrequency on item: " + e.getMessage(), e);
            }
        }
    }

    public T selectNextWeightedItem(List<T> items) {
        int index = binarySearchForNextWeightedIndex();
        return items.get(index);
    }

    private int binarySearchForNextWeightedIndex() {
        int low = 0;
        int high = cumulativeWeights.size() - 1;
        int randomValue = random.nextInt(totalWeight) + 1; // +1 to ensure the range is 1 to totalWeight inclusive

        while (low <= high) {
            int mid = (low + high) / 2;
            if (cumulativeWeights.get(mid) < randomValue) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return low;
    }
}