import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A distributed word-count example.
 */
public class DistributedWordCount {

  public static void main(String[] args) {
    // Initialize Ray runtime.
    Ray.init();
    Map<String, Integer> totalCounts = new HashMap<>();
    List<ObjectRef<Map<String, Integer>>> objectRefs = new ArrayList<>();
    for (int i = 0; i < WordCount.NUM_FILES; i++) {
      // Ray can turn any Java static method into a Ray remote function.
      // This will immediately return an `ObjectRef` (a future), and then spawn a task that runs remotely.
      // All these tasks will run in parallel.
      ObjectRef<Map<String, Integer>> objectRef = Ray.task(WordCount::countWordsInFile, "files/" + i + ".txt").remote();
      objectRefs.add(objectRef);
    }
    // Synchronously get all the results and merge them.
    Ray.get(objectRefs).forEach(counts -> {
      counts.forEach((word, count) -> {
        totalCounts.put(word, totalCounts.getOrDefault(word, 0) + count);
      });
    });
    // Print the top 10 most frequent words in all files.
    totalCounts.entrySet().stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .limit(10).forEach(entry -> {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    });
  }
}
